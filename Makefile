ENVRUN = poetry run

shell:
	@poetry shell

update:
	@poetry update

build:
	@docker-compose -f docker-compose.prod.yml build

build-dev:
	@docker-compose build

run:
	@docker-compose -f docker-compose.prod.yml up -d --build

run-dev:
	@docker-compose up -d --build

lint:
	@$(ENVRUN) flake8 iris --max-line-length 88

type:
	@$(ENVRUN) mypy --ignore-missing-imports iris

test:
	@$(ENVRUN) pytest tests

# Hack for running the compose stack on Fedora.
selinux:
	sudo setenforce 0
	mkdir -p volumes/grafana/    && sudo chown -R 472:472     volumes/grafana/
	mkdir -p volumes/loki/       && sudo chown -R 10001:10001 volumes/loki/
	mkdir -p volumes/prometheus/ && sudo chown -R 65534:65534 volumes/prometheus/
