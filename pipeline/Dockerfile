FROM gcr.io/dataflow-templates-base/flex-template-launcher-image

# Flex Template launcher image; the worker harness image is derived from the
# Beam SDK version in requirements.txt via `gcloud dataflow flex-template build`.
COPY requirements.txt /template/requirements.txt
COPY . /template/src

ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="/template/requirements.txt"
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/template/src/pipeline/main.py"
