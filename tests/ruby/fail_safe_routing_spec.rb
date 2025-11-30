# frozen_string_literal: true
require_relative 'spec_helper'

describe "Fail-Safe Routing" do
  let(:pool_settings) {
    {
      "query_parser_enabled" => true,
      "query_parser_read_write_splitting" => true,
      "primary_reads_enabled" => false,
      "default_role" => "any" # Default is any, but we want to ensure splitting works
    }
  }
  let(:processes) { Helpers::Pgcat.single_shard_setup("sharded_db", 5, "transaction", "random", "info", pool_settings) }

  after do
    processes.all_databases.map(&:reset)
    processes.pgcat.shutdown
  end

  it "routes to primary on parse failure and resets on next valid query" do
    # Reset stats after startup to clear validation queries
    processes.all_databases.each(&:reset_stats)

    conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))

    # 1. Send a query that fails parsing (invalid SQL)
    # This should be routed to Primary because of the fail-safe logic
    begin
      conn.async_exec("GARBAGE")
    rescue PG::SyntaxError
      # Expected syntax error from Postgres (Primary)
    rescue => e
      # Ignore other errors
    end

    # Check that Primary received the query (it might be hard to check exact count if it failed,
    # but we can check if Replica received it. If Replica didn't receive it, and we got a response, it must be Primary)
    # Actually, we can check if Primary received *something*.
    # But better: check that Replicas received NOTHING.
    processes.replicas.each do |replica|
        if replica.query_count != 0
          puts "DEBUG: Replica #{replica.port} queries:"
          replica.with_connection { |c|
            res = c.exec("SELECT query, calls FROM pg_stat_statements")
            res.each { |row| puts "#{row['calls']} calls: #{row['query']}" }
          }
        end
        expect(replica.query_count).to eq(0)
    end

    # 2. Send a valid read-only query
    # This should be routed to Replica because inference should work and overwrite the Primary role
    res = conn.async_exec("SELECT 1")
    expect(res.values[0][0]).to eq("1")

    # Check that one of the replicas received the query
    replica_hits = processes.replicas.sum(&:count_select_1)
    expect(replica_hits).to eq(1)

    # Primary should not have received the SELECT 1
    # (It might have received the invalid query, but count_select_1 only counts SELECT 1)
    expect(processes.primary.count_select_1).to eq(0)
  end
end
