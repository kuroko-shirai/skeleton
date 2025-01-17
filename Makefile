#/bin/bash
# Use Bash instead of SH
export SHELL := /bin/bash

.DEFAULT_GOAL := app

GOPATH := $(shell go env GOPATH)

CUR_DIR  := $(shell pwd)
APP_PATH := $(CUR_DIR)/cmd/app

# Run the docker
.PHONY: up
up:
	@echo "Up"
	@docker-compose up -d

# Run the docker
.PHONY: down
down:
	@echo "Down"
	@docker-compose down

# Run the app
.PHONY: app
app:
	@echo "App running..."
	@go run $(APP_PATH)/main.go

# Run the app with '-race' flag
.PHONY: race-app
race-app:
	@echo "App running with '-race' flag..."
	@go run -race $(APP_PATH)/main.go

# Run updating project's dependencies
.PHONY: update
update:
	@echo "Update dependencies..."
	@go mod tidy && go mod vendor
