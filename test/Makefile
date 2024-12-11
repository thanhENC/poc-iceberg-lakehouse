# Makefile to manage dataplatfom docker images

# VARIABLES
# -----------------------------------------------------------------------------
SPARK_APP_NAME=app_nessie.py

# Create docker network if not exists
# -----------------------------------------------------------------------------
create-network:
	@docker network inspect data-network > /dev/null 2>&1 || docker network create --driver bridge data-network
	@echo "Network data-network created"

# compose up with all services
# -----------------------------------------------------------------------------
compose-up-all: create-network
	@docker compose --profile spark --profile trino --env-file .env up -d
	@docker ps

# compose down then remove network
# -----------------------------------------------------------------------------
.PHONY: compose-down
compose-down:
	@docker compose --profile trino --profile spark down
	@docker network rm data-network
	@echo "Network data-network removed"

# Spark master submit
# -----------------------------------------------------------------------------
.PHONY: spark-submit
spark-submit:
	@docker exec -it spark-master bash -c "cd /opt/spark-apps && spark-submit --master spark://spark-master:7077 $(SPARK_APP_NAME)"
