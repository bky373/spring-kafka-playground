COMPOSE_FILE=compose.yaml

up:
	docker-compose -f $(COMPOSE_FILE) up -d --build

ul:
	docker-compose -f $(COMPOSE_FILE) up -d --build
	docker-compose -f $(COMPOSE_FILE) logs -f

down:
	docker-compose -f $(COMPOSE_FILE) down

l:
	docker-compose -f $(COMPOSE_FILE) logs -f

ps:
	docker-compose -f $(COMPOSE_FILE) ps

rebuild:
	docker-compose -f $(COMPOSE_FILE) down --rmi all
	docker-compose -f $(COMPOSE_FILE) up -d --build
