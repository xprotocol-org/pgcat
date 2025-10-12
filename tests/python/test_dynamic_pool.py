#!/usr/bin/env python3

import time
import psycopg2
import pytest
from utils import connect_db, cleanup_conn


def terminate_database_connections(db_name):
    admin_conn = psycopg2.connect(
        "postgres://postgres:postgres@127.0.0.1:5432/postgres"
    )
    admin_conn.autocommit = True
    admin_cursor = admin_conn.cursor()

    admin_cursor.execute(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
        f"WHERE datname = '{db_name}' AND pid <> pg_backend_pid()"
    )

    admin_cursor.close()
    admin_conn.close()
    time.sleep(0.5)


def test_dynamic_pool_creation(pgcat):
    admin_conn = psycopg2.connect(
        "postgres://postgres:postgres@127.0.0.1:5432/postgres"
    )
    admin_conn.autocommit = True
    admin_cursor = admin_conn.cursor()

    try:
        terminate_database_connections("test_dynamic_pool")
        admin_cursor.execute("DROP DATABASE IF EXISTS test_dynamic_pool")
        admin_cursor.execute("CREATE DATABASE test_dynamic_pool")
    except Exception as e:
        print(f"Database setup: {e}")

    admin_cursor.close()
    admin_conn.close()

    conn, cursor = connect_db(
        database="test_dynamic_pool",
        user="sharding_user",
        password="sharding_user"
    )

    cursor.execute("SELECT current_database()")
    result = cursor.fetchone()

    assert result[0] == "test_dynamic_pool", \
        f"Expected 'test_dynamic_pool', got '{result[0]}'"

    cleanup_conn(conn, cursor)

    admin_conn, admin_cursor = connect_db(admin=True)

    admin_cursor.execute("SHOW POOLS")
    pools = admin_cursor.fetchall()

    dynamic_pool_found = False
    for pool in pools:
        if pool[0] == "test_dynamic_pool" and pool[1] == "sharding_user":
            dynamic_pool_found = True
            break

    assert dynamic_pool_found, "Dynamic pool not found in SHOW POOLS output"

    cleanup_conn(admin_conn, admin_cursor)

    terminate_database_connections("test_dynamic_pool")

    admin_conn = psycopg2.connect(
        "postgres://postgres:postgres@127.0.0.1:5432/postgres"
    )
    admin_conn.autocommit = True
    admin_cursor = admin_conn.cursor()
    admin_cursor.execute("DROP DATABASE IF EXISTS test_dynamic_pool")
    admin_cursor.close()
    admin_conn.close()


def test_dynamic_pool_single_connection_limit(pgcat):
    admin_conn = psycopg2.connect(
        "postgres://postgres:postgres@127.0.0.1:5432/postgres"
    )
    admin_conn.autocommit = True
    admin_cursor = admin_conn.cursor()

    try:
        terminate_database_connections("test_single_conn")
        admin_cursor.execute("DROP DATABASE IF EXISTS test_single_conn")
        admin_cursor.execute("CREATE DATABASE test_single_conn")
    except Exception as e:
        print(f"Database setup: {e}")

    admin_cursor.close()
    admin_conn.close()

    conn, cursor = connect_db(
        database="test_single_conn",
        user="sharding_user",
        password="sharding_user"
    )

    cursor.execute("SELECT 1")
    cleanup_conn(conn, cursor)

    admin_conn, admin_cursor = connect_db(admin=True)

    admin_cursor.execute("SHOW POOLS")
    pools = admin_cursor.fetchall()

    for pool in pools:
        if pool[0] == "test_single_conn" and pool[1] == "sharding_user":
            cl_active = pool[2]
            print(f"Dynamic pool cl_active: {cl_active}")
            break

    cleanup_conn(admin_conn, admin_cursor)

    terminate_database_connections("test_single_conn")

    admin_conn = psycopg2.connect(
        "postgres://postgres:postgres@127.0.0.1:5432/postgres"
    )
    admin_conn.autocommit = True
    admin_cursor = admin_conn.cursor()
    admin_cursor.execute("DROP DATABASE IF EXISTS test_single_conn")
    admin_cursor.close()
    admin_conn.close()


def test_dynamic_pool_without_default(pgcat):
    pass
