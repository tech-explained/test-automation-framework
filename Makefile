.PHONY: migrate unit smoke regression dry-run report clean

ifndef HR_PG_DSN
$(error HR_PG_DSN is not set)
endif

migrate:
	bash scripts/migrate.sh

unit:
	python3 -m pytest tests/unit -q

smoke:
	python3 -m test_framework.runner --env local --suite smoke

regression:
	python3 -m test_framework.runner --env local --suite regression

dry-run:
	python3 -m test_framework.runner --env local --suite regression --dry-run

clean:
	rm -rf test_framework/reports/*.md
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
