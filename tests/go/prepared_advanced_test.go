package pgcat

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	_ "github.com/lib/pq"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// TestAdvanced tests prepared statement caching scenarios that could trigger
// the race condition found in pgcat where:
// 1. Pool cache assigns statement names (e.g., PGCAT_100)
// 2. Server cache tracks which statements exist on each server connection
// 3. DEALLOCATE ALL clears server cache but pool cache remains
// 4. LRU eviction tries to CLOSE statements that don't exist on server
//
// These tests use GORM with pgx/v5 stdlib driver and raw pgx connections
// to simulate real-world usage patterns that could expose cache desync bugs.
func TestAdvanced(t *testing.T) {
	t.Cleanup(setup(t))

	t.Run("GORM prepared statements with cache eviction", testGormPreparedStatements)
	t.Run("pgx prepared statements with cache eviction", testPgxPreparedStatements)
	t.Run("Concurrent prepared statements stress test", testConcurrentPreparedStatements)
	t.Run("DEALLOCATE ALL simulation", testDeallocateAllScenario)
	t.Run("Cache overflow with 1000+ statements", testCacheOverflow)
	t.Run("Template database with pg_terminate_backend", testTemplateDatabaseWithTerminate)
}

func buildShardedDSN() string {
	return fmt.Sprintf(
		"host=localhost port=%d user=sharding_user password=sharding_user dbname=sharded_db sslmode=disable",
		port,
	)
}

func openGormWithCustomLogger(dsn string, t *testing.T) (*gorm.DB, *sql.DB, error) {
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse connection string: %w", err)
	}

	sqlDB := stdlib.OpenDB(*config)
	if sqlDB == nil {
		return nil, nil, fmt.Errorf("failed to open SQL connection")
	}

	gormDB, err := gorm.Open(
		postgres.New(postgres.Config{Conn: sqlDB}),
		&gorm.Config{
			PrepareStmt:            true,
			SkipDefaultTransaction: true,
			Logger: logger.New(
				&testLogWriter{t: t},
				logger.Config{
					SlowThreshold:             200 * time.Millisecond,
					LogLevel:                  logger.Info,
					IgnoreRecordNotFoundError: false,
					Colorful:                  true,
				},
			),
		},
	)
	if err != nil {
		sqlDB.Close()
		return nil, nil, fmt.Errorf("failed to open GORM connection: %w", err)
	}

	return gormDB, sqlDB, nil
}

func testGormPreparedStatements(t *testing.T) {
	db, sqlDB, err := openGormWithCustomLogger(buildShardedDSN(), t)
	if err != nil {
		t.Fatalf("failed to open connection: %+v", err)
	}
	defer sqlDB.Close()

	sqlDB.SetMaxIdleConns(5)
	sqlDB.SetMaxOpenConns(10)

	ctx := context.Background()

	// Execute many different queries to potentially fill cache
	t.Log("Executing varied queries to populate cache...")
	for i := 0; i < 100; i++ {
		var result int
		// Each query is slightly different to create new prepared statements
		query := fmt.Sprintf("SELECT %d + $1 as result", i)
		err := db.WithContext(ctx).Raw(query, i).Scan(&result).Error
		if err != nil {
			t.Fatalf("query %d failed: %+v", i, err)
		}

		expected := i + i
		if result != expected {
			t.Fatalf("query %d: expected %d, got %d", i, expected, result)
		}
	}

	t.Log("Re-executing earlier queries to test cache hits...")
	// Now re-execute some earlier queries - they should hit cache
	for i := 0; i < 10; i++ {
		var result int
		query := fmt.Sprintf("SELECT %d + $1 as result", i)
		err := db.WithContext(ctx).Raw(query, i).Scan(&result).Error
		if err != nil {
			t.Fatalf("re-query %d failed: %+v", i, err)
		}
	}

	t.Log("GORM test completed successfully")
}

// testPgxPreparedStatements tests pgx with prepared statement caching
func testPgxPreparedStatements(t *testing.T) {
	dsn := fmt.Sprintf(
		"postgres://sharding_user:sharding_user@localhost:%d/sharded_db?sslmode=disable",
		port,
	)

	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("ParseConfig failed: %+v", err)
	}

	// Configure connection pool
	config.MaxConns = 5
	config.MinConns = 1

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatalf("NewWithConfig failed: %+v", err)
	}
	defer pool.Close()

	ctx := context.Background()

	t.Log("Executing varied queries with pgx...")
	for i := 0; i < 100; i++ {
		var result int
		query := fmt.Sprintf("SELECT %d + $1::int as result", i)
		err := pool.QueryRow(ctx, query, i).Scan(&result)
		if err != nil {
			t.Fatalf("query %d failed: %+v", i, err)
		}

		expected := i + i
		if result != expected {
			t.Fatalf("query %d: expected %d, got %d", i, expected, result)
		}
	}

	t.Log("Re-executing queries to test prepared statement reuse...")
	for i := 0; i < 10; i++ {
		var result int
		query := fmt.Sprintf("SELECT %d + $1::int as result", i)
		err := pool.QueryRow(ctx, query, i).Scan(&result)
		if err != nil {
			t.Fatalf("re-query %d failed: %+v", i, err)
		}
	}

	t.Log("pgx test completed successfully")
}

func testConcurrentPreparedStatements(t *testing.T) {
	dsn := fmt.Sprintf(
		"postgres://sharding_user:sharding_user@localhost:%d/sharded_db?sslmode=disable",
		port,
	)

	const numGoroutines = 10
	const queriesPerGoroutine = 50

	var wg sync.WaitGroup
	errChan := make(chan error, numGoroutines)

	t.Logf("Starting %d concurrent connections, each executing %d queries...", numGoroutines, queriesPerGoroutine)

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			// Each goroutine gets its own connection using pgx stdlib
			sqlDB, err := sql.Open("pgx", dsn)
			if err != nil {
				errChan <- fmt.Errorf("goroutine %d: open failed: %w", goroutineID, err)
				return
			}
			defer sqlDB.Close()

			db, err := gorm.Open(
				postgres.New(postgres.Config{Conn: sqlDB}),
				&gorm.Config{
					PrepareStmt: true,
					Logger:      logger.Default.LogMode(logger.Silent),
				},
			)
			if err != nil {
				errChan <- fmt.Errorf("goroutine %d: gorm.Open failed: %w", goroutineID, err)
				return
			}

			ctx := context.Background()

			// Execute queries with varied patterns
			for i := 0; i < queriesPerGoroutine; i++ {
				var result int
				// Mix of shared and unique queries
				queryID := (goroutineID*queriesPerGoroutine + i) % 200
				query := fmt.Sprintf("SELECT %d + $1 as result", queryID)
				err := db.WithContext(ctx).Raw(query, i).Scan(&result).Error
				if err != nil {
					errChan <- fmt.Errorf("goroutine %d, query %d failed: %w", goroutineID, i, err)
					return
				}
			}
		}(g)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Fatalf("Concurrent test failed: %+v", err)
	}

	t.Log("Concurrent test completed successfully")
}

func testDeallocateAllScenario(t *testing.T) {
	dsn := fmt.Sprintf(
		"postgres://sharding_user:sharding_user@localhost:%d/sharded_db?sslmode=disable",
		port,
	)

	sqlDB, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("could not open connection: %+v", err)
	}
	defer sqlDB.Close()

	sqlDB.SetMaxOpenConns(1)
	sqlDB.SetMaxIdleConns(1)

	db, err := gorm.Open(
		postgres.New(postgres.Config{Conn: sqlDB}),
		&gorm.Config{
			PrepareStmt: true,
			Logger:      logger.Default.LogMode(logger.Info),
		},
	)
	if err != nil {
		t.Fatalf("gorm.Open failed: %+v", err)
	}

	ctx := context.Background()

	t.Log("Phase 1: Prepare statements in transaction mode...")
	for i := 0; i < 50; i++ {
		var result int
		query := fmt.Sprintf("SELECT %d + $1 as result", i)
		err := db.WithContext(ctx).Raw(query, i).Scan(&result).Error
		if err != nil {
			t.Fatalf("phase 1 query %d failed: %+v", i, err)
		}
	}

	t.Log("Phase 2: Force connection return and cleanup (sleep to trigger DEALLOCATE ALL)...")
	// Close GORM to return connections to pool
	sqlInner, _ := db.DB()
	sqlInner.Close()

	// Wait for cleanup
	time.Sleep(100 * time.Millisecond)

	t.Log("Phase 3: Re-open and try to use potentially deallocated statements...")
	sqlDB2, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("could not re-open connection: %+v", err)
	}
	defer sqlDB2.Close()

	db2, err := gorm.Open(
		postgres.New(postgres.Config{Conn: sqlDB2}),
		&gorm.Config{
			PrepareStmt: true,
			Logger:      logger.Default.LogMode(logger.Info),
		},
	)
	if err != nil {
		t.Fatalf("gorm.Open phase 3 failed: %+v", err)
	}

	// Try to execute same queries - pgcat might think they're cached
	for i := 0; i < 50; i++ {
		var result int
		query := fmt.Sprintf("SELECT %d + $1 as result", i)
		err := db2.WithContext(ctx).Raw(query, i).Scan(&result).Error
		if err != nil {
			t.Fatalf("phase 3 query %d failed: %+v", i, err)
		}
	}

	t.Log("DEALLOCATE ALL scenario test completed successfully")
}

func testCacheOverflow(t *testing.T) {
	db, sqlDB, err := openGormConnection(buildShardedDSN(), true, logger.Silent)
	if err != nil {
		t.Fatalf("failed to open connection: %+v", err)
	}
	defer sqlDB.Close()

	ctx := context.Background()

	t.Log("Executing 1200 unique queries to overflow cache...")
	// Execute more queries than cache can hold
	for i := 0; i < 1200; i++ {
		var result int
		// Each query is unique
		query := fmt.Sprintf("SELECT %d + $1 + $2 as result", i)
		err := db.WithContext(ctx).Raw(query, i, i*2).Scan(&result).Error
		if err != nil {
			t.Fatalf("overflow query %d failed: %+v", i, err)
		}

		if i%100 == 0 {
			t.Logf("Executed %d queries...", i)
		}
	}

	t.Log("Re-executing early queries (should be evicted from cache)...")
	// Now try queries that should have been evicted
	for i := 0; i < 100; i++ {
		var result int
		query := fmt.Sprintf("SELECT %d + $1 + $2 as result", i)
		err := db.WithContext(ctx).Raw(query, i, i*2).Scan(&result).Error
		if err != nil {
			t.Fatalf("evicted query %d failed: %+v", i, err)
		}
	}

	t.Log("Cache overflow test completed successfully")
}

func openGormConnection(dsn string, prepareStmt bool, logLevel logger.LogLevel) (*gorm.DB, *sql.DB, error) {
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse connection string: %w", err)
	}

	sqlDB := stdlib.OpenDB(*config)
	if sqlDB == nil {
		return nil, nil, fmt.Errorf("failed to open SQL connection")
	}

	gormDB, err := gorm.Open(
		postgres.New(postgres.Config{Conn: sqlDB}),
		&gorm.Config{
			PrepareStmt:            prepareStmt,
			SkipDefaultTransaction: true,
			Logger:                 logger.Default.LogMode(logLevel),
		},
	)
	if err != nil {
		sqlDB.Close()
		return nil, nil, fmt.Errorf("failed to open GORM connection: %w", err)
	}

	return gormDB, sqlDB, nil
}

// testTemplateDatabaseWithTerminate tests creating a template database,
// populating it, calling pg_terminate_backend, then creating a new DB from template.
// This tests prepared statement cache behavior when connections are terminated while
// statements are cached, then the same connection pool is used for a new database.
func testTemplateDatabaseWithTerminate(t *testing.T) {
	ctx := context.Background()
	timestamp := time.Now().Unix()
	templateDB := fmt.Sprintf("test_template_%d", timestamp)
	newDB := fmt.Sprintf("test_db_%d", timestamp)

	postgresDSN := fmt.Sprintf(
		"host=localhost port=%d user=postgres password=postgres dbname=postgres sslmode=disable",
		port,
	)
	postgresGormDB, postgresSQLDB, err := openGormConnection(postgresDSN, true, logger.Silent)
	if err != nil {
		t.Fatalf("failed to connect to postgres database: %+v", err)
	}
	defer postgresSQLDB.Close()

	t.Logf("Creating template database: %s", templateDB)
	err = postgresGormDB.WithContext(ctx).Exec(fmt.Sprintf("CREATE DATABASE %s", templateDB)).Error
	if err != nil {
		t.Fatalf("failed to create template database: %+v", err)
	}

	defer func() {
		directDSN := "host=localhost port=5432 user=postgres password=postgres dbname=postgres sslmode=disable"
		directGormDB, directSQLDB, err := openGormConnection(directDSN, false, logger.Silent)
		if err != nil {
			t.Logf("failed to connect for cleanup: %+v", err)
			return
		}
		defer directSQLDB.Close()

		_ = directGormDB.WithContext(ctx).Exec(fmt.Sprintf(
			"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname IN ('%s', '%s') AND pid <> pg_backend_pid()",
			templateDB, newDB,
		)).Error
		time.Sleep(100 * time.Millisecond)

		_ = directGormDB.WithContext(ctx).Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", newDB)).Error
		_ = directGormDB.WithContext(ctx).Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", templateDB)).Error
		t.Logf("Cleaned up databases: %s, %s", templateDB, newDB)
	}()

	t.Logf("Connecting to template database to run migrations")
	templateDSN := fmt.Sprintf(
		"host=localhost port=%d user=postgres password=postgres dbname=%s sslmode=disable",
		port, templateDB,
	)
	templateGormDB, templateSQLDB, err := openGormConnection(templateDSN, true, logger.Silent)
	if err != nil {
		t.Fatalf("failed to connect to template database: %+v", err)
	}

	t.Log("Running migrations on template database with prepared statements enabled")
	err = templateGormDB.WithContext(ctx).Exec(
		"CREATE TABLE test_table (id SERIAL PRIMARY KEY, name TEXT)",
	).Error
	if err != nil {
		t.Fatalf("failed to create table in template: %+v", err)
	}

	err = templateGormDB.WithContext(ctx).Exec(
		"INSERT INTO test_table (name) VALUES ($1)",
		"test_data",
	).Error
	if err != nil {
		t.Fatalf("failed to insert data in template: %+v", err)
	}

	err = templateGormDB.WithContext(ctx).Exec(
		"CREATE INDEX idx_name ON test_table(name)",
	).Error
	if err != nil {
		t.Fatalf("failed to create index in template: %+v", err)
	}

	var count int64
	err = templateGormDB.WithContext(ctx).Raw("SELECT COUNT(*) FROM test_table").Scan(&count).Error
	if err != nil {
		t.Fatalf("failed to count rows in template: %+v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 row in template, got %d", count)
	}
	t.Logf("Template database has %d row(s)", count)

	t.Log("Closing template database connections")
	templateSQLDB.Close()

	t.Logf("Immediately calling pg_terminate_backend")
	err = postgresGormDB.WithContext(ctx).Exec(fmt.Sprintf(
		"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '%s' AND pid <> pg_backend_pid()",
		templateDB,
	)).Error
	if err != nil {
		t.Fatalf("failed to terminate backends: %+v", err)
	}

	t.Logf("Immediately creating new database from template")
	err = postgresGormDB.WithContext(ctx).Exec(fmt.Sprintf("CREATE DATABASE %s WITH TEMPLATE %s", newDB, templateDB)).Error
	if err != nil {
		t.Fatalf("failed to create database from template: %+v", err)
	}

	t.Logf("Connecting to new database and performing operations that use prepared statements")
	newDBDSN := fmt.Sprintf(
		"host=localhost port=%d user=postgres password=postgres dbname=%s sslmode=disable",
		port, newDB,
	)
	newGormDB, newDBSQLDB, err := openGormConnection(newDBDSN, true, logger.Silent)
	if err != nil {
		t.Fatalf("failed to connect to new database: %+v", err)
	}
	defer newDBSQLDB.Close()

	var newCount int64
	err = newGormDB.WithContext(ctx).Raw("SELECT COUNT(*) FROM test_table").Scan(&newCount).Error
	if err != nil {
		t.Fatalf("failed to count rows in new db: %+v", err)
	}
	if newCount != 1 {
		t.Fatalf("expected 1 row in new db, got %d", newCount)
	}

	var name string
	err = newGormDB.WithContext(ctx).Raw("SELECT name FROM test_table LIMIT 1").Scan(&name).Error
	if err != nil {
		t.Fatalf("failed to query name from new db: %+v", err)
	}
	if name != "test_data" {
		t.Fatalf("expected name 'test_data', got '%s'", name)
	}

	t.Log("Performing UPDATE operation with prepared statement on new database")
	err = newGormDB.WithContext(ctx).Exec(
		"UPDATE test_table SET name = $1 WHERE id = $2",
		"updated_data",
		1,
	).Error
	if err != nil {
		t.Fatalf("failed to update row in new db: %+v", err)
	}

	err = newGormDB.WithContext(ctx).Raw("SELECT name FROM test_table WHERE id = 1").Scan(&name).Error
	if err != nil {
		t.Fatalf("failed to query updated name from new db: %+v", err)
	}
	if name != "updated_data" {
		t.Fatalf("expected name 'updated_data', got '%s'", name)
	}

	t.Log("Performing multiple INSERT operations to trigger more prepared statement caching")
	for i := 2; i <= 5; i++ {
		err = newGormDB.WithContext(ctx).Exec(
			"INSERT INTO test_table (name) VALUES ($1)",
			fmt.Sprintf("row_%d", i),
		).Error
		if err != nil {
			t.Fatalf("failed to insert row %d in new db: %+v", i, err)
		}
	}

	err = newGormDB.WithContext(ctx).Raw("SELECT COUNT(*) FROM test_table").Scan(&newCount).Error
	if err != nil {
		t.Fatalf("failed to count final rows in new db: %+v", err)
	}
	if newCount != 5 {
		t.Fatalf("expected 5 rows in new db after inserts, got %d", newCount)
	}

	t.Logf("Successfully verified template database pattern: count=%d, updated=%s", newCount, name)
	t.Log("Template database test with pg_terminate_backend completed successfully")
}

// testLogWriter implements io.Writer to redirect GORM logs to testing.T
type testLogWriter struct {
	t *testing.T
}

func (w *testLogWriter) Printf(format string, args ...interface{}) {
	w.t.Logf(format, args...)
}
