package pgcat

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
    "time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

func TestSearchPathIsolation(t *testing.T) {
    // Setup schemas
    setupSchemas(t)
    // defer teardownSchemas(t) // Keep for debugging if needed

    // We need to ensure we reuse the same server connection to trigger the bug.
    // We can do this by running sequentially and ensuring the pool allows reuse.
    
    // Client A
    // We use search_path in the connection string which sends it in Startup packet.
    // pgcat should sync this to the server.
    dsnA := fmt.Sprintf("host=localhost port=%d user=sharding_user password=sharding_user dbname=sharded_db sslmode=disable search_path=schema_a", port)
    connA := openDB(t, dsnA)
    defer connA.Close()

    // Client B
    dsnB := fmt.Sprintf("host=localhost port=%d user=sharding_user password=sharding_user dbname=sharded_db sslmode=disable search_path=schema_b", port)
    connB := openDB(t, dsnB)
    defer connB.Close()

    ctx := context.Background()

    // 1. Client A prepares and executes
    // We use a transaction to ensure we hold the connection
    txA, err := connA.BeginTx(ctx, nil)
    if err != nil {
        t.Fatalf("Begin A failed: %v", err)
    }
    
    // Get PID to verify reuse later (optional, but good for debugging)
    var pidA int
    err = txA.QueryRow("SELECT pg_backend_pid()").Scan(&pidA)
    if err != nil {
        t.Fatalf("Get PID A failed: %v", err)
    }
    t.Logf("Client A PID: %d", pidA)

    stmtA, err := txA.Prepare("SELECT val FROM test_table WHERE id = $1")
    if err != nil {
        t.Fatalf("Prepare A failed: %v", err)
    }
    var valA string
    err = stmtA.QueryRow(1).Scan(&valA)
    if err != nil {
        t.Fatalf("Query A failed: %v", err)
    }
    if valA != "Value A" {
        t.Errorf("Expected 'Value A', got '%s'", valA)
    }
    stmtA.Close()
    txA.Commit()

    // Sleep briefly to ensure connection is returned to pool and available
    time.Sleep(100 * time.Millisecond)

    // 2. Client B prepares and executes (hopefully reusing same server)
    txB, err := connB.BeginTx(ctx, nil)
    if err != nil {
        t.Fatalf("Begin B failed: %v", err)
    }

    var pidB int
    err = txB.QueryRow("SELECT pg_backend_pid()").Scan(&pidB)
    if err != nil {
        t.Fatalf("Get PID B failed: %v", err)
    }
    t.Logf("Client B PID: %d", pidB)

    if pidA == pidB {
        t.Log("Successfully reused the same server connection!")
    } else {
        t.Log("Warning: Got different server connection. Bug might not reproduce if cache is not shared or if server was clean.")
    }

    // The query string MUST be identical to trigger the cache hit in pgcat
    stmtB, err := txB.Prepare("SELECT val FROM test_table WHERE id = $1")
    if err != nil {
        t.Fatalf("Prepare B failed: %v", err)
    }
    var valB string
    err = stmtB.QueryRow(1).Scan(&valB)
    if err != nil {
        t.Fatalf("Query B failed: %v", err)
    }
    
    if valB != "Value B" {
        t.Errorf("BUG DETECTED: Expected 'Value B' (from schema_b), got '%s' (likely from schema_a)", valB)
    } else {
        t.Log("Success: Got correct value from schema_b")
    }
    stmtB.Close()
    txB.Commit()
}

func setupSchemas(t *testing.T) {
    dsn := fmt.Sprintf("host=localhost port=%d user=sharding_user password=sharding_user dbname=sharded_db sslmode=disable", port)
    db := openDB(t, dsn)
    defer db.Close()

    _, err := db.Exec("DROP SCHEMA IF EXISTS schema_a CASCADE")
    if err != nil { t.Fatal(err) }
    _, err = db.Exec("DROP SCHEMA IF EXISTS schema_b CASCADE")
    if err != nil { t.Fatal(err) }

    _, err = db.Exec("CREATE SCHEMA schema_a")
    if err != nil { t.Fatal(err) }
    _, err = db.Exec("CREATE TABLE schema_a.test_table (id int, val text)")
    if err != nil { t.Fatal(err) }
    _, err = db.Exec("INSERT INTO schema_a.test_table VALUES (1, 'Value A')")
    if err != nil { t.Fatal(err) }

    _, err = db.Exec("CREATE SCHEMA schema_b")
    if err != nil { t.Fatal(err) }
    _, err = db.Exec("CREATE TABLE schema_b.test_table (id int, val text)")
    if err != nil { t.Fatal(err) }
    _, err = db.Exec("INSERT INTO schema_b.test_table VALUES (1, 'Value B')")
    if err != nil { t.Fatal(err) }
}

func openDB(t *testing.T, dsn string) *sql.DB {
    config, err := pgx.ParseConfig(dsn)
    if err != nil {
        t.Fatalf("ParseConfig failed: %v", err)
    }
    db := stdlib.OpenDB(*config)
    return db
}
