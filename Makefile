COMPOSE := docker compose -f infra/docker-compose.yml

.PHONY: up up-all down clean smoke smoke-all ps logs e2e

up:            ## Start the CDC stack (Postgres, Kafka, Debezium)
	$(COMPOSE) --profile cdc up -d --wait
	$(COMPOSE) --profile cdc run --rm connect-init

up-all:        ## Start everything, including Airflow
	$(COMPOSE) --profile cdc --profile orchestration up -d --wait
	$(COMPOSE) --profile cdc run --rm connect-init

smoke:         ## Check the CDC stack end to end
	infra/smoke.sh cdc

smoke-all:     ## Check every service
	infra/smoke.sh cdc orchestration

ps:
	$(COMPOSE) --profile cdc --profile orchestration ps

logs:
	$(COMPOSE) --profile cdc --profile orchestration logs -f

down:          ## Stop containers, keep nothing running
	$(COMPOSE) --profile cdc --profile orchestration down

clean:         ## Stop containers and delete their data
	$(COMPOSE) --profile cdc --profile orchestration down -v

e2e:           ## Full pipeline on the CDC stack: Postgres/Kafka -> bronze -> silver -> gold
	$(MAKE) up
	infra/e2e-data.sh
	uv run pytest tests_e2e -v
