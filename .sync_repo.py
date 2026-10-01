#!/usr/bin/env python3
"""Sync a local dir to tech-explained/test-automation-framework via Git Data API.

Creates blobs -> tree (base = current main tree) -> commit -> update ref.
Handles additions, modifications, AND deletions. Retries transient failures.
"""
from __future__ import annotations

import base64
import json
import os
import sys
import time
import urllib.request
import urllib.error

sys.path.insert(0, "/opt/hatch/skills/skill-creator/bin")
from dynamic_credentials import (
    add_surrogate_to_request,
    read_json_response,
    read_response_body,
)

CRED = "custom.github"
HOSTS = ("api.github.com",)
API = "https://api.github.com"
OWNER, REPO, BRANCH = "tech-explained", "test-automation-framework", "main"
WORKSPACE = os.path.expanduser("~/workspace/hr-dataflow-gcp")

SKIP_DIRS = {".git", "__pycache__", ".pytest_cache", ".venv", "node_modules"}
SKIP_FILES = {".DS_Store"}
SKIP_PREFIXES = ("test_framework/reports/",)


def api(method, path, payload=None, retries=5):
    data = json.dumps(payload).encode() if payload is not None else None
    for attempt in range(retries):
        req = urllib.request.Request(API + path, data=data, method=method)
        req.add_header("Accept", "application/vnd.github+json")
        req.add_header("X-GitHub-Api-Version", "2022-11-28")
        if data:
            req.add_header("Content-Type", "application/json")
        add_surrogate_to_request(req, CRED, allowed_hosts=HOSTS)
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                return resp.status, read_json_response(resp)
        except urllib.error.HTTPError as e:
            try:
                body = json.loads(read_response_body(e).decode())
            except Exception:
                body = {"message": f"HTTP {e.code}"}
            if e.code in (429, 502, 503) and attempt < retries - 1:
                time.sleep(2 ** attempt)
                continue
            return e.code, body
        except Exception:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
                continue
            raise


def collect_files():
    files = {}
    for root, dirs, names in os.walk(WORKSPACE):
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
        for n in names:
            if n in SKIP_FILES:
                continue
            full = os.path.join(root, n)
            rel = os.path.relpath(full, WORKSPACE)
            if rel.startswith(SKIP_PREFIXES):
                continue
            with open(full, "rb") as f:
                files[rel] = f.read()
    return files


def main():
    message = sys.argv[1] if len(sys.argv) > 1 else "sync workspace"

    code, ref = api("GET", f"/repos/{OWNER}/{REPO}/git/ref/heads/{BRANCH}")
    assert code == 200, f"ref: {code} {ref}"
    old_commit_sha = ref["object"]["sha"]
    code, old_commit = api("GET", f"/repos/{OWNER}/{REPO}/git/commits/{old_commit_sha}")
    assert code == 200, f"commit: {code}"
    base_tree_sha = old_commit["tree"]["sha"]

    code, tree = api("GET", f"/repos/{OWNER}/{REPO}/git/trees/{base_tree_sha}?recursive=1")
    assert code == 200, f"tree: {code}"
    old_paths = {t["path"]: t for t in tree.get("tree", []) if t["type"] == "blob"}

    new_files = collect_files()
    print(f"local files: {len(new_files)}, remote blobs: {len(old_paths)}", flush=True)

    tree_entries = []
    changed = 0
    for rel in sorted(new_files):
        content = new_files[rel]
        b64 = base64.b64encode(content).decode()
        code, blob = api("POST", f"/repos/{OWNER}/{REPO}/git/blobs",
                         {"content": b64, "encoding": "base64"})
        assert code == 201, f"blob {rel}: {code} {blob}"
        tree_entries.append({"path": rel, "mode": "100644",
                             "type": "blob", "sha": blob["sha"]})
        changed += 1
        if changed % 5 == 0:
            time.sleep(1)
    print(f"blobs created: {changed}", flush=True)

    deleted = [p for p in old_paths if p not in new_files]
    for p in deleted:
        tree_entries.append({"path": p, "mode": "100644",
                             "type": "blob", "sha": None})
    print(f"deletions: {len(deleted)}", flush=True)
    if deleted:
        print("deleted:", ", ".join(deleted[:12]), flush=True)

    code, new_tree = api("POST", f"/repos/{OWNER}/{REPO}/git/trees",
                         {"base_tree": base_tree_sha, "tree": tree_entries})
    assert code == 201, f"tree: {code} {new_tree}"
    print(f"tree: {new_tree['sha'][:8]}", flush=True)

    code, commit = api("POST", f"/repos/{OWNER}/{REPO}/git/commits",
                       {"message": message, "tree": new_tree["sha"],
                        "parents": [old_commit_sha]})
    assert code == 201, f"commit: {code} {commit}"
    print(f"commit: {commit['sha'][:8]}", flush=True)

    code, _ = api("PATCH", f"/repos/{OWNER}/{REPO}/git/refs/heads/{BRANCH}",
                  {"sha": commit["sha"]})
    assert code == 200, f"ref update: {code}"
    print(f"pushed {commit['sha'][:8]}: {message}", flush=True)


if __name__ == "__main__":
    main()
