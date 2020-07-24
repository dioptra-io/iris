ENVRUN = poetry run

shell:
	@poetry shell

update:
	@poetry update

run:
	@docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build

run-dev:
	@docker-compose -f docker-compose.yml -f docker-compose.dev.yml up -d --build

lint:
	@$(ENVRUN) flake8 iris --max-line-length 88

type:
	@$(ENVRUN) mypy --ignore-missing-imports iris

test:
	@$(ENVRUN) pytest