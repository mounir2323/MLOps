.PHONY: help setup start stop status clean

# Configuration
LAKEFS_PORT := 8000
MLFLOW_PORT := 5003
DAGSTER_PORT := 3000
VENV_PATH := .venv

# Couleurs
GREEN := \033[0;32m
YELLOW := \033[1;33m
RED := \033[0;31m
NC := \033[0m

help: ## Affiche ce message d'aide
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "$(GREEN)%-10s$(NC) %s\n", $$1, $$2}'

setup: ## Configuration et installation
	@echo "$(GREEN)⚙️  Setup...$(NC)"
	@python -m venv $(VENV_PATH)
	@$(VENV_PATH)/bin/pip install -r requirements.txt
	@cd ml_pipeline && ../$(VENV_PATH)/bin/python setup_lakefs_branches.py
	@mkdir -p data/incoming data/processed
	@echo "$(GREEN)✅ Ready$(NC)"

start: ## Lance tous les services
	@echo "$(GREEN)🚀 Starting services...$(NC)"
	@if ! docker info > /dev/null 2>&1; then \
		echo "$(YELLOW)⚠️  Starting Docker daemon...$(NC)"; \
		open --background -a Docker; \
		sleep 10; \
	fi
	@cd ml_pipeline && docker-compose up -d
	@$(VENV_PATH)/bin/mlflow server --host 0.0.0.0 --port $(MLFLOW_PORT) > /dev/null 2>&1 &
	@cd ml_pipeline && ../$(VENV_PATH)/bin/dagster dev --port $(DAGSTER_PORT) > /dev/null 2>&1 &
	@sleep 5
	@echo "$(GREEN)✅ Services started$(NC)"
	@echo "$(YELLOW)LakeFS:  http://localhost:$(LAKEFS_PORT)$(NC)"
	@echo "$(YELLOW)MLflow:  http://localhost:$(MLFLOW_PORT)$(NC)"
	@echo "$(YELLOW)Dagster: http://localhost:$(DAGSTER_PORT)$(NC)"

status: ## Vérifie le statut
	@echo "$(GREEN)Status:$(NC)"
	@echo -n "LakeFS:  "; if lsof -ti:$(LAKEFS_PORT) > /dev/null; then echo "$(GREEN)Running$(NC)"; else echo "$(RED)Stopped$(NC)"; fi
	@echo -n "MLflow:  "; if lsof -ti:$(MLFLOW_PORT) > /dev/null; then echo "$(GREEN)Running$(NC)"; else echo "$(RED)Stopped$(NC)"; fi
	@echo -n "Dagster: "; if lsof -ti:$(DAGSTER_PORT) > /dev/null; then echo "$(GREEN)Running$(NC)"; else echo "$(RED)Stopped$(NC)"; fi

stop: ## Arrête tous les services
	@echo "$(GREEN)🛑 Stopping...$(NC)"
	@if docker info > /dev/null 2>&1; then \
		cd ml_pipeline && docker-compose down; \
	else \
		echo "$(YELLOW)⚠️  Docker not running, skipping docker-compose down$(NC)"; \
	fi
	@lsof -ti:$(MLFLOW_PORT) | xargs kill -9 2>/dev/null || true
	@lsof -ti:$(DAGSTER_PORT) | xargs kill -9 2>/dev/null || true
	@echo "$(GREEN)✅ Stopped$(NC)"

clean: stop ## Nettoie les fichiers temporaires
	@echo "$(GREEN)🧹 Cleaning...$(NC)"
	@if docker info > /dev/null 2>&1; then \
		docker system prune -f --volumes=false; \
	else \
		echo "$(YELLOW)⚠️  Docker not running, skipping docker cleanup$(NC)"; \
	fi
	@rm -f *.log *.pid
	@find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
	@echo "$(GREEN)✅ Clean$(NC)"

# Ajout d'utilitaires de debugging LakeFS au Makefile

debug-lakefs: ## Debug des branches et contenus LakeFS
	@echo "🔍 Debugging LakeFS..."
	@echo "📁 Contenu de la branche main:"
	lakectl fs ls lakefs://mlops-spoty-data/main/
	@echo "\n📁 Contenu de la branche incoming:"
	lakectl fs ls lakefs://mlops-spoty-data/incoming/ || echo "Branche incoming vide ou inexistante"
	@echo "\n📁 Contenu de la branche staging:"
	lakectl fs ls lakefs://mlops-spoty-data/staging/ || echo "Branche staging vide ou inexistante"

check-sensors: ## Vérifier ce que surveillent les sensors
	@echo "🔍 Vérification des dossiers surveillés par les sensors..."
	@echo "📂 Dossier incoming/raw/ (surveillé par lakefs_incoming_sensor):"
	lakectl fs ls lakefs://mlops-spoty-data/incoming/raw/ || echo "Dossier vide - le sensor attend des fichiers ici"
	@echo "\n📂 Jobs terminés (surveillés par preprocessing_to_training_sensor):"
	@echo "   → Le sensor surveille les runs Dagster, pas un dossier LakeFS"

list-assets: ## Lister tous les assets dans LakeFS
	@echo "📊 Assets dans la branche main:"
	@echo "Raw data:"
	lakectl fs ls lakefs://mlops-spoty-data/main/raw/ || echo "Pas de raw data"
	@echo "Processed data:"
	lakectl fs ls lakefs://mlops-spoty-data/main/processed/ || echo "Pas de processed data"
	@echo "Models:"
	lakectl fs ls lakefs://mlops-spoty-data/main/models/ || echo "Pas de models"