# frozen_string_literal: true
require_relative 'spec_helper'

describe "Dynamic Pool Creation" do
  let(:processes) { Helpers::Pgcat.single_instance_setup("simple_db", 1) }

  after do
    processes.all_databases.map(&:reset)
    processes.pgcat.shutdown
  end

  context "when default pool is configured" do
    it "creates dynamic pool for new database" do
      current_configs = processes.pgcat.current_config

      default_pool_config = current_configs["pools"]["simple_db"].clone
      default_pool_config["shards"]["0"]["database"] = "postgres"

      default_pool_config["users"]["1"] = {
        "username" => "simple_user",
        "password" => "simple_user",
        "pool_size" => 15
      }

      current_configs["pools"]["default"] = default_pool_config

      processes.pgcat.update_config(current_configs)
      processes.pgcat.reload_config

      sleep 1

      admin_pg_conn = PG.connect(
        host: "localhost",
        port: 5432,
        user: "postgres",
        password: "postgres",
        dbname: "postgres"
      )
      admin_pg_conn.async_exec("DROP DATABASE IF EXISTS dynamic_test_db")
      admin_pg_conn.async_exec("CREATE DATABASE dynamic_test_db")
      admin_pg_conn.async_exec("GRANT ALL ON DATABASE dynamic_test_db TO simple_user")
      admin_pg_conn.close

      conn = PG::connect(
        host: processes.pgcat.host,
        port: processes.pgcat.port,
        user: "simple_user",
        password: "simple_user",
        dbname: "dynamic_test_db"
      )

      result = conn.async_exec("SELECT current_database()")
      expect(result[0]["current_database"]).to eq("dynamic_test_db")

      conn.close

      admin_conn = PG::connect(processes.pgcat.admin_connection_string)
      pools_result = admin_conn.async_exec("SHOW POOLS")

      dynamic_pool_exists = pools_result.any? do |row|
        row["database"] == "dynamic_test_db" && row["user"] == "simple_user"
      end

      expect(dynamic_pool_exists).to be true

      admin_conn.close
    end

    it "inherits pool size from default pool configuration" do
      current_configs = processes.pgcat.current_config

      default_pool_config = current_configs["pools"]["simple_db"].clone
      default_pool_config["shards"]["0"]["database"] = "postgres"

      default_pool_config["users"]["1"] = {
        "username" => "simple_user",
        "password" => "simple_user",
        "pool_size" => 15
      }

      current_configs["pools"]["default"] = default_pool_config

      processes.pgcat.update_config(current_configs)
      processes.pgcat.reload_config

      sleep 1

      admin_pg_conn = PG.connect(
        host: "localhost",
        port: 5432,
        user: "postgres",
        password: "postgres",
        dbname: "postgres"
      )
      admin_pg_conn.async_exec("DROP DATABASE IF EXISTS dynamic_pool_size_test")
      admin_pg_conn.async_exec("CREATE DATABASE dynamic_pool_size_test")
      admin_pg_conn.async_exec("GRANT ALL ON DATABASE dynamic_pool_size_test TO simple_user")
      admin_pg_conn.close

      conn1 = PG::connect(
        host: processes.pgcat.host,
        port: processes.pgcat.port,
        user: "simple_user",
        password: "simple_user",
        dbname: "dynamic_pool_size_test"
      )

      conn1.async_exec("BEGIN")

      admin_conn = PG::connect(processes.pgcat.admin_connection_string)
      pools_result = admin_conn.async_exec("SHOW POOLS")

      dynamic_pool = pools_result.find do |row|
        row["database"] == "dynamic_pool_size_test" && row["user"] == "simple_user"
      end

      expect(dynamic_pool).not_to be_nil
      expect(dynamic_pool["cl_active"].to_i).to eq(1)
      expect(dynamic_pool["cl_idle"].to_i + dynamic_pool["cl_active"].to_i).to be <= 15

      conn1.async_exec("COMMIT")
      conn1.close
      admin_conn.close
    end

    it "creates separate dynamic pools for different users" do
      current_configs = processes.pgcat.current_config

      default_pool_config = current_configs["pools"]["simple_db"].clone
      default_pool_config["shards"]["0"]["database"] = "postgres"

      default_pool_config["users"]["1"] = {
        "username" => "simple_user",
        "password" => "simple_user",
        "pool_size" => 15
      }

      current_configs["pools"]["default"] = default_pool_config

      processes.pgcat.update_config(current_configs)
      processes.pgcat.reload_config

      sleep 1

      admin_pg_conn = PG.connect(
        host: "localhost",
        port: 5432,
        user: "postgres",
        password: "postgres",
        dbname: "postgres"
      )
      admin_pg_conn.async_exec("DROP DATABASE IF EXISTS multi_user_db")
      admin_pg_conn.async_exec("CREATE DATABASE multi_user_db")
      admin_pg_conn.async_exec("GRANT ALL ON DATABASE multi_user_db TO simple_user")
      admin_pg_conn.close

      conn1 = PG::connect(
        host: processes.pgcat.host,
        port: processes.pgcat.port,
        user: "simple_user",
        password: "simple_user",
        dbname: "multi_user_db"
      )

      result1 = conn1.async_exec("SELECT current_user")
      expect(result1[0]["current_user"]).to eq("simple_user")

      conn1.close

      admin_conn = PG::connect(processes.pgcat.admin_connection_string)
      pools_result = admin_conn.async_exec("SHOW POOLS")

      user_pools = pools_result.select do |row|
        row["database"] == "multi_user_db"
      end

      expect(user_pools.count).to be >= 1

      admin_conn.close
    end

    it "executes queries successfully through dynamic pool" do
      current_configs = processes.pgcat.current_config

      default_pool_config = current_configs["pools"]["simple_db"].clone
      default_pool_config["shards"]["0"]["database"] = "postgres"

      default_pool_config["users"]["1"] = {
        "username" => "simple_user",
        "password" => "simple_user",
        "pool_size" => 15
      }

      current_configs["pools"]["default"] = default_pool_config

      processes.pgcat.update_config(current_configs)
      processes.pgcat.reload_config

      sleep 1

      admin_pg_conn = PG.connect(
        host: "localhost",
        port: 5432,
        user: "postgres",
        password: "postgres",
        dbname: "postgres"
      )
      admin_pg_conn.async_exec("DROP DATABASE IF EXISTS dynamic_query_test_db")
      admin_pg_conn.async_exec("CREATE DATABASE dynamic_query_test_db")
      admin_pg_conn.async_exec("GRANT ALL ON DATABASE dynamic_query_test_db TO simple_user")
      admin_pg_conn.close

      conn = PG::connect(
        host: processes.pgcat.host,
        port: processes.pgcat.port,
        user: "simple_user",
        password: "simple_user",
        dbname: "dynamic_query_test_db"
      )

      conn.async_exec("CREATE TABLE IF NOT EXISTS test_data (id SERIAL PRIMARY KEY, value TEXT)")
      conn.async_exec("INSERT INTO test_data (value) VALUES ('test1'), ('test2'), ('test3')")

      result = conn.async_exec("SELECT value FROM test_data ORDER BY id")

      expect(result.ntuples).to eq(3)
      expect(result[0]["value"]).to eq("test1")
      expect(result[1]["value"]).to eq("test2")
      expect(result[2]["value"]).to eq("test3")

      update_result = conn.async_exec("UPDATE test_data SET value = 'updated' WHERE id = 2")
      expect(update_result.cmd_tuples).to eq(1)

      verify_result = conn.async_exec("SELECT value FROM test_data WHERE id = 2")
      expect(verify_result[0]["value"]).to eq("updated")

      conn.async_exec("DROP TABLE test_data")
      conn.close
    end
  end

  context "when default pool is configured but user is not in it" do
    it "rejects connection to dynamic database" do
      current_configs = processes.pgcat.current_config

      default_pool_config = current_configs["pools"]["simple_db"].clone
      default_pool_config["shards"]["0"]["database"] = "postgres"
      current_configs["pools"]["default"] = default_pool_config

      processes.pgcat.update_config(current_configs)
      processes.pgcat.reload_config

      sleep 1

      expect do
        PG::connect(
          host: processes.pgcat.host,
          port: processes.pgcat.port,
          user: "simple_user",
          password: "simple_user",
          dbname: "dynamic_test_db",
          connect_timeout: 2
        )
      end.to raise_error(PG::ConnectionBad, /User 'simple_user' not found in default pool configuration/)
    end
  end

  context "when default pool is not configured" do
    it "rejects connection to non-configured database" do
      expect do
        PG::connect(
          host: processes.pgcat.host,
          port: processes.pgcat.port,
          user: "simple_user",
          password: "simple_user",
          dbname: "non_existent_db",
          connect_timeout: 2
        )
      end.to raise_error(PG::ConnectionBad, /No pool configured for database/)
    end
  end

  context "when user doesn't exist in default pool" do
    it "rejects connection" do
      current_configs = processes.pgcat.current_config

      default_pool_config = current_configs["pools"]["simple_db"].clone
      default_pool_config["shards"]["0"]["database"] = "postgres"
      default_pool_config["users"] = {
        "0" => {
          "username" => "different_user",
          "password" => "different_pass",
          "pool_size" => 15
        }
      }
      current_configs["pools"]["default"] = default_pool_config

      processes.pgcat.update_config(current_configs)
      processes.pgcat.reload_config

      sleep 1

      expect do
        PG::connect(
          host: processes.pgcat.host,
          port: processes.pgcat.port,
          user: "simple_user",
          password: "simple_user",
          dbname: "test_db",
          connect_timeout: 2
        )
      end.to raise_error(PG::ConnectionBad)
    end
  end

  shared_examples "handles pg_terminate_backend gracefully" do |pool_mode|
    let(:mode_suffix) { pool_mode == "session" ? "_session" : "" }

    def setup_default_pool_with_mode(processes, pool_mode)
      current_configs = processes.pgcat.current_config

      default_pool_config = current_configs["pools"]["simple_db"].clone
      default_pool_config["shards"]["0"]["database"] = "postgres"
      default_pool_config["pool_mode"] = pool_mode

      default_pool_config["users"]["0"] = {
        "username" => "postgres",
        "password" => "postgres",
        "pool_size" => 5
      }

      default_pool_config["users"]["1"] = {
        "username" => "simple_user",
        "password" => "simple_user",
        "pool_size" => 15
      }

      current_configs["pools"]["default"] = default_pool_config

      processes.pgcat.update_config(current_configs)
      processes.pgcat.reload_config

      sleep 0.5
    end

    it "does not fail when creating database from template after terminating template connections" do
      setup_default_pool_with_mode(processes, pool_mode)

      template_db = "test_template#{mode_suffix}_#{Time.now.to_i}"
      new_db = "test_db#{mode_suffix}_#{Time.now.to_i}"

      admin_conn = PG::connect(processes.pgcat.admin_connection_string)

      begin
        # Use pgcat connection to postgres database (via default pool) with postgres superuser
        admin_pgcat_conn = PG.connect(
          host: processes.pgcat.host,
          port: processes.pgcat.port,
          user: "postgres",
          password: "postgres",
          dbname: "postgres"
        )
        admin_pgcat_conn.async_exec("CREATE DATABASE #{template_db}")
        admin_pgcat_conn.close

        template_conn = PG.connect(
          host: processes.pgcat.host,
          port: processes.pgcat.port,
          user: "simple_user",
          password: "simple_user",
          dbname: template_db
        )
        template_conn.async_exec("CREATE TABLE test_table (id SERIAL PRIMARY KEY, name TEXT)")
        template_conn.async_exec("INSERT INTO test_table (name) VALUES ('test_data')")
        result = template_conn.async_exec("SELECT COUNT(*) FROM test_table")
        expect(result[0]['count'].to_i).to eq(1)
        template_conn.close

        # Use pgcat connection with postgres user for terminating backends and creating database
        admin_pgcat_conn = PG.connect(
          host: processes.pgcat.host,
          port: processes.pgcat.port,
          user: "postgres",
          password: "postgres",
          dbname: "postgres"
        )
        admin_pgcat_conn.async_exec("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '#{template_db}' AND pid <> pg_backend_pid()")
        admin_pgcat_conn.async_exec("CREATE DATABASE #{new_db} WITH TEMPLATE #{template_db}")
        admin_pgcat_conn.close

        new_db_conn = PG.connect(
          host: processes.pgcat.host,
          port: processes.pgcat.port,
          user: "simple_user",
          password: "simple_user",
          dbname: new_db
        )
        result = new_db_conn.async_exec("SELECT COUNT(*) FROM test_table")
        expect(result[0]['count'].to_i).to eq(1)

        result = new_db_conn.async_exec("SELECT name FROM test_table LIMIT 1")
        expect(result[0]['name']).to eq('test_data')

        new_db_conn.close

        bans = admin_conn.async_exec("SHOW BANS").to_a
        expect(bans.count).to eq(0)

      ensure
        begin
          cleanup_conn = PG.connect(
            host: "localhost",
            port: 5432,
            user: "postgres",
            password: "postgres",
            dbname: "postgres"
          )
          cleanup_conn.async_exec("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname IN ('#{template_db}', '#{new_db}') AND pid <> pg_backend_pid()")
          cleanup_conn.async_exec("DROP DATABASE IF EXISTS #{new_db}")
          cleanup_conn.async_exec("DROP DATABASE IF EXISTS #{template_db}")
          cleanup_conn.close
        rescue => e
          puts "Cleanup error: #{e.message}"
        end
        admin_conn.close
      end
    end

    it "handles rapid database creation and connection attempts" do
      setup_default_pool_with_mode(processes, pool_mode)

      template_db = "test_template_rapid#{mode_suffix}_#{Time.now.to_i}"
      new_dbs = 3.times.map { |i| "test_db_rapid#{mode_suffix}_#{Time.now.to_i}_#{i}" }

      admin_conn = PG::connect(processes.pgcat.admin_connection_string)

      begin
        # Create template database through pgcat with postgres user
        admin_pgcat_conn = PG.connect(
          host: processes.pgcat.host,
          port: processes.pgcat.port,
          user: "postgres",
          password: "postgres",
          dbname: "postgres"
        )
        admin_pgcat_conn.async_exec("CREATE DATABASE #{template_db}")
        admin_pgcat_conn.close

        # Connect to template through dynamic pool and add data
        template_conn = PG.connect(
          host: processes.pgcat.host,
          port: processes.pgcat.port,
          user: "simple_user",
          password: "simple_user",
          dbname: template_db
        )
        template_conn.async_exec("CREATE TABLE test_table (id SERIAL PRIMARY KEY, value INT)")
        template_conn.async_exec("INSERT INTO test_table (value) VALUES (42)")
        template_conn.close

        new_dbs.each do |new_db|
          # Terminate template connections and create new database through pgcat
          admin_pgcat_conn = PG.connect(
            host: processes.pgcat.host,
            port: processes.pgcat.port,
            user: "postgres",
            password: "postgres",
            dbname: "postgres"
          )
          admin_pgcat_conn.async_exec("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '#{template_db}' AND pid <> pg_backend_pid()")
          admin_pgcat_conn.async_exec("CREATE DATABASE #{new_db} WITH TEMPLATE #{template_db}")
          admin_pgcat_conn.close

          # Immediately connect to new database through dynamic pool
          new_conn = PG.connect(
            host: processes.pgcat.host,
            port: processes.pgcat.port,
            user: "simple_user",
            password: "simple_user",
            dbname: new_db
          )
          result = new_conn.async_exec("SELECT value FROM test_table LIMIT 1")
          expect(result[0]['value'].to_i).to eq(42)
          new_conn.close
        end

        bans = admin_conn.async_exec("SHOW BANS").to_a
        expect(bans.count).to eq(0)

      ensure
        begin
          cleanup_conn = PG.connect(
            host: "localhost",
            port: 5432,
            user: "postgres",
            password: "postgres",
            dbname: "postgres"
          )
          new_dbs.each do |db|
            cleanup_conn.async_exec("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '#{db}' AND pid <> pg_backend_pid()")
            cleanup_conn.async_exec("DROP DATABASE IF EXISTS #{db}")
          end
          cleanup_conn.async_exec("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '#{template_db}' AND pid <> pg_backend_pid()")
          cleanup_conn.async_exec("DROP DATABASE IF EXISTS #{template_db}")
          cleanup_conn.close
        rescue => e
          puts "Cleanup error: #{e.message}"
        end
        admin_conn.close
      end
    end

    it "handles repeated template creation and termination cycles" do
      setup_default_pool_with_mode(processes, pool_mode)

      admin_conn = PG::connect(processes.pgcat.admin_connection_string)

      5.times do |cycle|
        template_db = "test_template_cycle#{mode_suffix}_#{Time.now.to_i}_#{cycle}"
        new_db = "test_db_cycle#{mode_suffix}_#{Time.now.to_i}_#{cycle}"

        begin
          pg_conn = PG.connect(
            host: processes.pgcat.host,
            port: processes.pgcat.port,
            user: "postgres",
            password: "postgres",
            dbname: "postgres"
          )
          pg_conn.async_exec("CREATE DATABASE #{template_db}")
          pg_conn.close

          template_conn = PG.connect(
            host: processes.pgcat.host,
            port: processes.pgcat.port,
            user: "postgres",
            password: "postgres",
            dbname: template_db
          )
          template_conn.async_exec("CREATE TABLE test_table (id SERIAL PRIMARY KEY, data TEXT)")
          template_conn.async_exec("INSERT INTO test_table (data) VALUES ('cycle_#{cycle}')")
          template_conn.async_exec("GRANT ALL ON TABLE test_table TO simple_user")
          template_conn.async_exec("GRANT ALL ON DATABASE #{template_db} TO simple_user")
          template_conn.close

          pg_conn = PG.connect(
            host: processes.pgcat.host,
            port: processes.pgcat.port,
            user: "postgres",
            password: "postgres",
            dbname: "postgres"
          )
          pg_conn.async_exec("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '#{template_db}' AND pid <> pg_backend_pid()")
          pg_conn.async_exec("CREATE DATABASE #{new_db} WITH TEMPLATE #{template_db}")
          pg_conn.close

          new_db_conn = PG.connect(
            host: processes.pgcat.host,
            port: processes.pgcat.port,
            user: "simple_user",
            password: "simple_user",
            dbname: new_db
          )
          result = new_db_conn.async_exec("SELECT data FROM test_table LIMIT 1")
          expect(result[0]['data']).to eq("cycle_#{cycle}")
          new_db_conn.close

          bans = admin_conn.async_exec("SHOW BANS").to_a
          expect(bans.count).to eq(0)

        ensure
          begin
            cleanup_conn = PG.connect(
              host: "localhost",
              port: 5432,
              user: "postgres",
              password: "postgres",
              dbname: "postgres"
            )
            cleanup_conn.async_exec("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname IN ('#{template_db}', '#{new_db}') AND pid <> pg_backend_pid()")
            cleanup_conn.async_exec("DROP DATABASE IF EXISTS #{new_db}")
            cleanup_conn.async_exec("DROP DATABASE IF EXISTS #{template_db}")
            cleanup_conn.close
          rescue => e
            puts "Cleanup error in cycle #{cycle}: #{e.message}"
          end
        end

        sleep 0.1
      end

      admin_conn.close
    end
  end

  context "with database template creation in transaction mode" do
    include_examples "handles pg_terminate_backend gracefully", "transaction"

    it "handles static pool for database that doesn't exist yet" do
      admin_pg_conn = PG.connect(
        host: "localhost",
        port: 5432,
        user: "postgres",
        password: "postgres",
        dbname: "postgres"
      )
      admin_pg_conn.async_exec("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'nonexistent_static_db'")
      admin_pg_conn.async_exec("DROP DATABASE IF EXISTS nonexistent_static_db")
      admin_pg_conn.close

      current_configs = processes.pgcat.current_config

      static_pool_config = current_configs["pools"]["simple_db"].clone
      static_pool_config["shards"]["0"]["database"] = "nonexistent_static_db"

      static_pool_config["users"]["0"] = {
        "username" => "simple_user",
        "password" => "simple_user",
        "pool_size" => 5
      }

      current_configs["pools"]["nonexistent_static_db"] = static_pool_config

      processes.pgcat.update_config(current_configs)
      processes.pgcat.reload_config

      sleep 2

      expect {
        PG::connect(
          host: processes.pgcat.host,
          port: processes.pgcat.port,
          user: "simple_user",
          password: "simple_user",
          dbname: "nonexistent_static_db"
        )
      }.to raise_error(PG::Error, /Pool down for database/)

      admin_pg_conn = PG.connect(
        host: "localhost",
        port: 5432,
        user: "postgres",
        password: "postgres",
        dbname: "postgres"
      )
      admin_pg_conn.async_exec("DROP DATABASE IF EXISTS nonexistent_static_db")
      admin_pg_conn.async_exec("CREATE DATABASE nonexistent_static_db")
      admin_pg_conn.async_exec("GRANT ALL ON DATABASE nonexistent_static_db TO simple_user")
      admin_pg_conn.close

      sleep 1

      conn = PG::connect(
        host: processes.pgcat.host,
        port: processes.pgcat.port,
        user: "simple_user",
        password: "simple_user",
        dbname: "nonexistent_static_db"
      )

      result = conn.async_exec("SELECT current_database()")
      expect(result[0]["current_database"]).to eq("nonexistent_static_db")

      conn.close

      admin_pg_conn = PG.connect(
        host: "localhost",
        port: 5432,
        user: "postgres",
        password: "postgres",
        dbname: "postgres"
      )
      admin_pg_conn.async_exec("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'nonexistent_static_db'")
      admin_pg_conn.async_exec("DROP DATABASE IF EXISTS nonexistent_static_db")
      admin_pg_conn.close
    end
  end

  context "with database template creation in session mode" do
    include_examples "handles pg_terminate_backend gracefully", "session"

    it "validates connections on checkout and retries with fresh connection after pg_terminate_backend" do
      current_configs = processes.pgcat.current_config

      default_pool_config = current_configs["pools"]["simple_db"].clone
      default_pool_config["shards"]["0"]["database"] = "postgres"
      default_pool_config["pool_mode"] = "session"

      default_pool_config["users"]["0"] = {
        "username" => "postgres",
        "password" => "postgres",
        "pool_size" => 5
      }

      default_pool_config["users"]["1"] = {
        "username" => "simple_user",
        "password" => "simple_user",
        "pool_size" => 5
      }

      current_configs["pools"]["default"] = default_pool_config

      processes.pgcat.update_config(current_configs)
      processes.pgcat.reload_config

      sleep 0.5

      test_db = "test_session_validation_#{Time.now.to_i}"

      admin_conn = PG::connect(processes.pgcat.admin_connection_string)

      begin
        admin_pgcat_conn = PG.connect(
          host: processes.pgcat.host,
          port: processes.pgcat.port,
          user: "postgres",
          password: "postgres",
          dbname: "postgres"
        )
        admin_pgcat_conn.async_exec("CREATE DATABASE #{test_db}")
        admin_pgcat_conn.close

        conn1 = PG.connect(
          host: processes.pgcat.host,
          port: processes.pgcat.port,
          user: "simple_user",
          password: "simple_user",
          dbname: test_db
        )
        conn1.async_exec("CREATE TABLE test_data (id SERIAL PRIMARY KEY, value TEXT)")
        conn1.async_exec("INSERT INTO test_data (value) VALUES ('initial')")
        conn1.close

        admin_pgcat_conn = PG.connect(
          host: processes.pgcat.host,
          port: processes.pgcat.port,
          user: "postgres",
          password: "postgres",
          dbname: "postgres"
        )
        result = admin_pgcat_conn.async_exec("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '#{test_db}' AND pid <> pg_backend_pid()")
        terminated_count = result.ntuples
        puts "Terminated #{terminated_count} backend(s) for database #{test_db}"
        admin_pgcat_conn.close

        sleep 0.5

        conn2 = PG.connect(
          host: processes.pgcat.host,
          port: processes.pgcat.port,
          user: "simple_user",
          password: "simple_user",
          dbname: test_db
        )
        result = conn2.async_exec("SELECT value FROM test_data LIMIT 1")
        expect(result[0]['value']).to eq('initial')

        conn2.async_exec("INSERT INTO test_data (value) VALUES ('after_terminate')")
        result = conn2.async_exec("SELECT COUNT(*) FROM test_data")
        expect(result[0]['count'].to_i).to eq(2)
        conn2.close

        bans = admin_conn.async_exec("SHOW BANS").to_a
        expect(bans.count).to eq(0)

      ensure
        begin
          cleanup_conn = PG.connect(
            host: "localhost",
            port: 5432,
            user: "postgres",
            password: "postgres",
            dbname: "postgres"
          )
          cleanup_conn.async_exec("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '#{test_db}' AND pid <> pg_backend_pid()")
          cleanup_conn.async_exec("DROP DATABASE IF EXISTS #{test_db}")
          cleanup_conn.close
        rescue => e
          puts "Cleanup error: #{e.message}"
        end
        admin_conn.close
      end
    end
  end
end

