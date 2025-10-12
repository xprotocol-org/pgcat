# frozen_string_literal: true
require_relative 'spec_helper'

describe "Random Load Balancing" do
  let(:processes) { Helpers::Pgcat.single_shard_setup("sharded_db", 5) }
  after do
    processes.all_databases.map(&:reset)
    processes.pgcat.shutdown
  end

  context "under regular circumstances" do
    it "balances query volume between all instances" do
      conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))

      query_count = QUERY_COUNT
      expected_share = query_count / processes.all_databases.count
      failed_count = 0

      query_count.times do
        conn.async_exec("SELECT 1 + 2")
      rescue
        failed_count += 1
      end

      expect(failed_count).to eq(0)
      processes.all_databases.map(&:count_select_1_plus_2).each do |instance_share|
        expect(instance_share).to be_within(expected_share * MARGIN_OF_ERROR).of(expected_share)
      end
    end
  end

  context "when some replicas are down" do
    it "balances query volume between working instances" do
      conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
      expected_share = QUERY_COUNT / (processes.all_databases.count - 2)
      failed_count = 0

      processes[:replicas][0].take_down do
        processes[:replicas][1].take_down do
          QUERY_COUNT.times do
            conn.async_exec("SELECT 1 + 2")
          rescue
            conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
            failed_count += 1
          end
        end
      end

      processes.all_databases.each do |instance|
        queries_routed = instance.count_select_1_plus_2
        if processes.replicas[0..1].include?(instance)
          expect(queries_routed).to eq(0)
        else
          expect(queries_routed).to be_within(expected_share * MARGIN_OF_ERROR).of(expected_share)
        end
      end
    end
  end

  context "when all replicas are down " do
    let(:processes) { Helpers::Pgcat.single_shard_setup("sharded_db", 5, "transaction", "random", "debug", {"default_role" => "replica"}) }

    it "unbans them automatically to prevent false positives in health checks that could make all replicas unavailable" do
      conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
      failed_count = 0
      number_of_replicas = processes[:replicas].length

      # Take down all replicas
      processes[:replicas].each(&:take_down)

      (number_of_replicas + 1).times do |n|
        conn.async_exec("SELECT 1 + 2")
      rescue
        conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
        failed_count += 1
      end

      expect(failed_count).to eq(number_of_replicas + 1)
      failed_count = 0

      # Ban_time is configured to 60 so this reset will only work
      # if the replicas are unbanned automatically
      processes[:replicas].each(&:reset)

      number_of_replicas.times do
        conn.async_exec("SELECT 1 + 2")
      rescue
        conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
        failed_count += 1
      end
      expect(failed_count).to eq(0)
    end
  end

  context "when clients disconnect proactively" do
    let(:processes) { Helpers::Pgcat.single_shard_setup("sharded_db", 5, "transaction", "random", "debug", {"default_role" => "replica"}) }

    it "does not ban servers when clients disconnect" do
      admin_conn = PG::connect(processes.pgcat.admin_connection_string)

      bans_before = admin_conn.async_exec("SHOW BANS").to_a
      expect(bans_before.count).to eq(0)

      5.times do
        conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
        conn.async_exec("SELECT 1 + 2")
        conn.close
      end

      sleep(0.2)

      bans_after = admin_conn.async_exec("SHOW BANS").to_a
      expect(bans_after.count).to eq(0)

      admin_conn.close
    end

    it "does not ban servers when clients disconnect during transaction" do
      admin_conn = PG::connect(processes.pgcat.admin_connection_string)

      bans_before = admin_conn.async_exec("SHOW BANS").to_a
      expect(bans_before.count).to eq(0)

      5.times do
        conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
        conn.async_exec("BEGIN")
        conn.async_exec("SELECT 1 + 2")
        conn.close
      end

      sleep(0.2)

      bans_after = admin_conn.async_exec("SHOW BANS").to_a
      expect(bans_after.count).to eq(0)

      admin_conn.close
    end

    it "does not ban servers when clients disconnect abruptly" do
      admin_conn = PG::connect(processes.pgcat.admin_connection_string)

      bans_before = admin_conn.async_exec("SHOW BANS").to_a
      expect(bans_before.count).to eq(0)

      3.times do
        random_string = (0...12).map { (65 + rand(26)).chr }.join
        connection_string = processes.pgcat.connection_string("sharded_db", "sharding_user", parameters: {"application_name" => random_string})

        pid = Process.spawn("psql -Atx #{connection_string} -c 'SELECT pg_sleep(2)' >/dev/null 2>&1")
        sleep(0.3)

        `pkill -9 -f '#{random_string}'`
        Process.wait(pid) rescue nil
      end

      sleep(0.5)

      bans_after = admin_conn.async_exec("SHOW BANS").to_a
      expect(bans_after.count).to eq(0)

      admin_conn.close
    end
  end

end

describe "Least Outstanding Queries Load Balancing" do
  let(:processes) { Helpers::Pgcat.single_shard_setup("sharded_db", 1, "transaction", "loc") }
  after do
    processes.all_databases.map(&:reset)
    processes.pgcat.shutdown
  end

  context "under homogeneous load" do
    it "balances query volume between all instances" do
      conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))

      query_count = QUERY_COUNT
      expected_share = query_count / processes.all_databases.count
      failed_count = 0

      query_count.times do
        conn.async_exec("SELECT 1 + 2")
      rescue
        failed_count += 1
      end

      expect(failed_count).to eq(0)
      processes.all_databases.map(&:count_select_1_plus_2).each do |instance_share|
        expect(instance_share).to be_within(expected_share * MARGIN_OF_ERROR).of(expected_share)
      end
    end
  end

  context "under heterogeneous load" do
    xit "balances query volume between all instances based on how busy they are" do
      slow_query_count = 2
      threads = Array.new(slow_query_count) do
        Thread.new do
          conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
          conn.async_exec("BEGIN")
        end
      end

      conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))

      query_count = QUERY_COUNT
      expected_share = query_count / (processes.all_databases.count - slow_query_count)
      failed_count = 0

      query_count.times do
        conn.async_exec("SELECT 1 + 2")
      rescue
        failed_count += 1
      end

      expect(failed_count).to eq(0)
      # Under LOQ, we expect replicas running the slow pg_sleep
      # to get no selects
      expect(
        processes.
          all_databases.
          map(&:count_select_1_plus_2).
          count { |instance_share| instance_share == 0 }
      ).to eq(slow_query_count)

      # We also expect the quick queries to be spread across
      # the idle servers only
      processes.
        all_databases.
        map(&:count_select_1_plus_2).
        reject { |instance_share| instance_share == 0 }.
        each do |instance_share|
          expect(instance_share).to be_within(expected_share * MARGIN_OF_ERROR).of(expected_share)
      end

      threads.map(&:join)
    end
  end

  context "when some replicas are down" do
    it "balances query volume between working instances" do
      conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
      expected_share = QUERY_COUNT / (processes.all_databases.count - 2)
      failed_count = 0

      processes[:replicas][0].take_down do
        processes[:replicas][1].take_down do
          QUERY_COUNT.times do
            conn.async_exec("SELECT 1 + 2")
          rescue
            conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
            failed_count += 1
          end
        end
      end

      expect(failed_count).to be <= 2
      processes.all_databases.each do |instance|
        queries_routed = instance.count_select_1_plus_2
        if processes.replicas[0..1].include?(instance)
          expect(queries_routed).to eq(0)
        else
          expect(queries_routed).to be_within(expected_share * MARGIN_OF_ERROR).of(expected_share)
        end
      end
    end
  end
end
