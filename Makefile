.PHONY: help test-up test-down test-clean test test-unit test test-coverage build vet

# Connection string the tests pick up from DB_CONNECTION_STRING. Matches the
# compose file: user "outbox", db "outbox", port 5434 on localhost.
TEST_DB_URL := postgres://outbox:outbox@localhost:5434/outbox?sslmode=disable

help:
	@echo "Targets:"
	@echo "  test-up        Start the local test Postgres (docker-compose)."
	@echo "  test-down      Stop the test Postgres (volumes preserved)."
	@echo "  test-clean     Stop and wipe the test Postgres (drops the volume)."
	@echo "  test           Run the full test suite against the test Postgres."
	@echo "  test-unit      Run only the unit tests (no DB required)."
	@echo "  test-coverage  Run the full suite with coverage output."
	@echo "  build          Build all packages."
	@echo "  vet            Run go vet across all packages."

# Bring up the local Postgres. Waits for healthy.
test-up:
	docker compose up -d --wait postgres

# Stop the container; keep the volume so the schema survives restarts.
test-down:
	docker compose down

# Stop and drop the volume; next test-up will re-run migrations from scratch.
test-clean:
	docker compose down -v

# Full test suite. Assumes test-up has run; will fail loudly otherwise.
test:
	DB_CONNECTION_STRING="$(TEST_DB_URL)" go test -tags=testing -count=1 ./...

# Unit-only subset: tests that do not require a real DB connection.
# (synctest-based + pure Go tests.)
test-unit:
	go test -tags=testing -count=1 -run "TestJSONBMap|TestWorker_ExitsOn" ./...

test-coverage:
	DB_CONNECTION_STRING="$(TEST_DB_URL)" go test -tags=testing -count=1 -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

build:
	go build ./...
	go build -tags=testing ./...

vet:
	go vet ./...
	go vet -tags=testing ./...
