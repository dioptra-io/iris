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