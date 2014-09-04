require "spec_helper"

require "cassandra_client"

describe CassandraClient do
  let(:client) {
    CassandraClient.new(connection_details: correct_connection_details)
  }

  let(:correct_connection_details) {
    {
      "node_ips"      => %w[localhost],
      "keyspace_name" => CassandraTestEnvironment.keyspace,
      "username"      => "cassandra",
      "password"      => "cassandra",
    }
  }

  def with_table(table_name)
    client.create_table(table_name)
    yield
    client.delete_table(table_name)
  end

  describe "#create_table" do
    it "creates new table" do
      expect(client.table_exists?("rspec")).to eql(false)
      with_table("rspec") do
        expect(client.table_exists?("rspec")).to eql(true)
      end
    end

    context "when table_name is part of the CQL language" do
      it "creates new table" do
        expect(client.table_exists?("table")).to eql(false)

        with_table("table") do
          expect(client.table_exists?("table")).to eql(true)
        end
      end
    end

    context "when table_name contains invalid characters" do
      it "raises InvalidTableName" do
        expect {
          client.create_table("5&@abc")
        }.to raise_exception(InvalidTableName)
      end
    end
  end

  describe "#delete_table" do
    context "when table does not exist" do
      it "does not raise exception" do
        random_table_name = SecureRandom.hex
        expect { client.delete_table(random_table_name) }.to_not raise_exception
      end
    end
  end

  describe "#store & #fetch" do
    it "reads and writes keys into/from table" do
      with_table("table") do
        client.store(table_name: "table", key: "stored_key", value: "stored_value")
        expect(client.fetch(table_name: "table", key: "stored_key")).to eql("stored_value")
      end
    end

    context "when table does not exist" do
      it "raises TableDoesNotExistException" do
        table_missing_exception = [
          TableDoesNotExistException,
          %{Table "inexistent_table" does not exist}
        ]
        
        expect {
          client.store(table_name: "inexistent_table", key: "stored_key", value: "stored_value")
        }.to raise_exception(*table_missing_exception)

        expect {
          client.fetch(table_name: "inexistent_table", key: "stored_key")
        }.to raise_exception(*table_missing_exception)
      end
    end

    context "when key does not exist" do
      it "raises KeyNotFoundException" do
        with_table("table") do
          expect {
            client.fetch(table_name: "table", key: "inexistent_key")
          }.to raise_exception(KeyNotFoundException, %{"inexistent_key" key not found})
        end
      end
    end
  end

  describe "#client" do
    it "memoizes" do
      expect(client.client).to eql(client.client)
    end

    context "when all the connection details are correct" do
      it "connects to Cassandra" do
        expect(client).to be_connected
      end
    end

    context "when connection details are not correct" do
      it "raises InvalidCassandraCredentialsException" do
        incorrect_credentials = {
          'node_ips'      => %w[localhost],
          "keyspace_name" => "system",
          "username"      => "inexistent",
          "password"      => "invalid",
        }

        expect { 
          CassandraClient.new(connection_details: incorrect_credentials).client
        }.to raise_exception(InvalidCassandraCredentialsException)
      end
    end

    context "when Cassandra is not available" do
      it "raises CassandraUnavailableException" do
        incorrect_cassandra_host = {
          'node_ips'            => %w[127.0.0.2],
          "keyspace_name"       => "system",
          "username"            => "cassandra",
          "password"            => "cassandra",
          "connection_timeout"  => 0.1
        }

        expect {
          CassandraClient.new(connection_details: incorrect_cassandra_host).client
        }.to raise_exception(CassandraUnavailableException)
      end
    end
  end
end
