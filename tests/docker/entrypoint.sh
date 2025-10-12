#!/bin/bash

set -e

if [ -d /app/target ]; then
    chmod -R 777 /app/target 2>/dev/null || true
fi

if [ ! -f /tmp/.deps_installed ]; then
    echo "=== Installing dependencies ==="

    if [ -d /app/tests/ruby ]; then
        echo "Installing Ruby dependencies..."
        cd /app/tests/ruby
        bundle install
    fi

    if [ -f /app/tests/python/requirements.txt ]; then
        echo "Installing Python dependencies..."
        cd /app
        pip3 install -r tests/python/requirements.txt --break-system-packages
    fi

    touch /tmp/.deps_installed
    echo "=== Dependencies installed ==="
fi

touch /tmp/.container_ready

exec "$@"

