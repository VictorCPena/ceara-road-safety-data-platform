.PHONY: build pipeline dbt shell clean docs

build:
	docker compose build

pipeline:
	docker compose run --rm pipeline ./scripts/run_pipeline.sh

dbt:
	docker compose run --rm pipeline dbt build --project-dir analytics --profiles-dir analytics

shell:
	docker compose run --rm pipeline bash

docs:
	docker compose run --rm --service-ports pipeline dbt docs serve --project-dir analytics --profiles-dir analytics --host 0.0.0.0

clean:
	docker compose down --remove-orphans
