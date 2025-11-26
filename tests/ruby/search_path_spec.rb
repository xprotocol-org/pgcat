require_relative 'spec_helper'

describe 'search_path prepared statement isolation' do
  before(:all) do
    # Setup schemas and tables
    admin_conn = PG.connect(
      host: '127.0.0.1',
      port: 5432,
      user: 'postgres',
      password: 'postgres',
      dbname: 'shard0'
    )

    admin_conn.exec("DROP SCHEMA IF EXISTS schema_a CASCADE")
    admin_conn.exec("DROP SCHEMA IF EXISTS schema_b CASCADE")
    
    admin_conn.exec("CREATE SCHEMA schema_a")
    admin_conn.exec("CREATE TABLE schema_a.test_table (id int, val text)")
    admin_conn.exec("INSERT INTO schema_a.test_table VALUES (1, 'Value A')")

    admin_conn.exec("CREATE SCHEMA schema_b")
    admin_conn.exec("CREATE TABLE schema_b.test_table (id int, val text)")
    admin_conn.exec("INSERT INTO schema_b.test_table VALUES (1, 'Value B')")
    
    admin_conn.exec("GRANT ALL ON SCHEMA schema_a TO sharding_user")
    admin_conn.exec("GRANT ALL ON TABLE schema_a.test_table TO sharding_user")
    admin_conn.exec("GRANT ALL ON SCHEMA schema_b TO sharding_user")
    admin_conn.exec("GRANT ALL ON TABLE schema_b.test_table TO sharding_user")

    admin_conn.close
  end

  after(:all) do
    admin_conn = PG.connect(
      host: '127.0.0.1',
      port: 5432,
      user: 'postgres',
      password: 'postgres',
      dbname: 'shard0'
    )
    admin_conn.exec("DROP SCHEMA IF EXISTS schema_a CASCADE")
    admin_conn.exec("DROP SCHEMA IF EXISTS schema_b CASCADE")
    admin_conn.close
  end

  it 'respects search_path for prepared statements' do
    # Client A
    conn_a = PG.connect(
      host: '127.0.0.1',
      port: 6432,
      user: 'sharding_user',
      password: 'sharding_user',
      dbname: 'sharded_db'
    )
    
    # Client B
    conn_b = PG.connect(
      host: '127.0.0.1',
      port: 6432,
      user: 'sharding_user',
      password: 'sharding_user',
      dbname: 'sharded_db'
    )

    # Prepare and execute in A
    conn_a.transaction do
      pid_a = conn_a.exec("SELECT pg_backend_pid()").getvalue(0, 0)
      puts "Client A PID: #{pid_a}"
      conn_a.exec("SET search_path TO schema_a")
      conn_a.prepare("my_stmt", "SELECT val FROM test_table WHERE id = $1")
      res_a = conn_a.exec_prepared("my_stmt", [1])
      expect(res_a.getvalue(0, 0)).to eq('Value A')
    end

    # Prepare and execute in B
    conn_b.transaction do
      pid_b = conn_b.exec("SELECT pg_backend_pid()").getvalue(0, 0)
      puts "Client B PID: #{pid_b}"
      conn_b.exec("SET search_path TO schema_b")
      conn_b.prepare("my_stmt", "SELECT val FROM test_table WHERE id = $1")
      res_b = conn_b.exec_prepared("my_stmt", [1])
      
      # If bug exists, this might return 'Value A' because it reused the prepared statement
      # that was compiled against schema_a
      expect(res_b.getvalue(0, 0)).to eq('Value B')
    end

    conn_a.close
    conn_b.close
  end
end
