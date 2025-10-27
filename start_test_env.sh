#!/bin/bash

set -e



show_help() {
    GREEN="\033[0;32m"
    RED="\033[0;31m"
    BLUE="\033[0;34m"
    RESET="\033[0m"

    echo "==================================="
    echo "Interactive test environment ready"
    echo "To run specific integration tests, you can use the following commands:"
    echo -e "   ${BLUE}Ruby:   ${RED}cd /app/tests/ruby && bundle exec ruby tests.rb --format documentation${RESET}"
    echo -e "   ${BLUE}Ruby:   ${RED}cd /app/tests/ruby && bundle exec rspec *_spec.rb --format documentation${RESET}"
    echo -e "   ${BLUE}Python: ${RED}cd /app/ && pytest${RESET}"
    echo -e "   ${BLUE}Rust:   ${RED}cd /app/tests/rust && cargo run${RESET}"
    echo -e "   ${BLUE}Go:     ${RED}cd /app/tests/go && /usr/local/go/bin/go test${RESET}"
    echo "the source code for tests are directly linked to the source code in the container so you can modify the code and run the tests again"
    echo "You can rebuild PgCat from within the container by running"
    echo -e "  ${GREEN}cargo build${RESET}"
    echo "You can also run all the integration tests by running"
    echo -e "  ${GREEN}INTERACTIVE_TEST_ENVIRONMENT="" bash /app/tests/docker/run.sh${RESET}"
    echo "and then run the tests again"
    echo "==================================="
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

cd "$SCRIPT_DIR/tests/docker"
docker compose kill main || true
docker compose build main
docker compose down
INTERACTIVE_TEST_ENVIRONMENT=true docker compose up -d --wait
echo "==================================="
docker compose exec -e LOG_LEVEL=error -d main toxiproxy-server
docker compose exec --workdir /app main cargo build
docker compose exec -d --workdir /app main ./target/debug/pgcat ./.circleci/pgcat.toml 2>&1 > /tmp/pgcat.log
docker compose exec --workdir /app/tests/ruby main bundle install
docker compose exec --workdir /app/tests/python main pip3 install -r requirements.txt --break-system-packages

show_help

docker compose exec --workdir /app/tests main bash -c "$(declare -f show_help); export -f show_help; bash"
