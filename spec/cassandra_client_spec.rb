require "spec_helper"

require "cassandra_client"

describe CassandraClient do
  let(:client) {
    CassandraClient.new(connection_details: correct_connection_details)
  }

  let(:correct_connection_details) {
    {
      "node_ips"      => node_ips,
      "username"      => "cassandra",
      "password"      => "cassandra",
    }
  }

  def with_keyspace(keyspace_name)
    client.create_keyspace(keyspace_name)
    yield
    client.delete_keyspace(keyspace_name)
  end

  def with_table(keyspace_name, table_name)
    client.create_table(keyspace_name, table_name)
    yield
    client.delete_keyspace(keyspace_name)
  end

  describe "#create_keyspace" do
    it "creates new keyspace" do
      expect(client.keyspace_exists?("rspeckeyspace")).to eql(false)
      with_keyspace("rspeckeyspace") do
        expect(client.keyspace_exists?("rspeckeyspace")).to eql(true)
      end
    end

    context "when keyspace_name contains invalid characters" do
      it "raises InvalidKeyspaceName" do
        expect {
          client.create_keyspace("5&@abc")
        }.to raise_exception(InvalidKeyspaceName)
      end
    end
  end

  describe "#create_table" do
    it "creates new table" do
      with_keyspace("keyspace_name") do
        expect(client.table_exists?("keyspace_name", "table_name")).to eql(false)
        with_table("keyspace_name", "table_name") do
          expect(client.table_exists?("keyspace_name", "table_name")).to eql(true)
        end
      end
    end

    context "when table_name is part of the CQL language" do
      it "creates new table" do
        with_keyspace("keyspace_name") do
          expect(client.table_exists?("keyspace_name", "table")).to eql(false)

          with_table("keyspace_name", "table") do
            expect(client.table_exists?("keyspace_name", "table")).to eql(true)
          end
        end
      end
    end

    context "when table_name contains invalid characters" do
      it "raises InvalidTableName" do
        expect {
          client.create_keyspace("keyspace_name")
          client.create_table("keyspace_name", "5&@abc")
        }.to raise_exception(InvalidTableName)
      end
    end
  end

  describe "#delete_keyspace" do
    context "when keyspace does not exist" do
      it "does not raise exception" do
        random_keyspace_name = SecureRandom.hex
        expect { client.delete_keyspace(random_keyspace_name) }.to_not raise_exception
      end
    end
  end

  describe "#store & #fetch" do
    it "reads and writes keys into/from table" do
      with_keyspace("keyspace_name") do
        with_table("keyspace_name", "table") do
          client.store(keyspace_name: "keyspace_name", table_name: "table", key: "stored_key", value: "stored_value")
          expect(client.fetch(keyspace_name: "keyspace_name", table_name: "table", key: "stored_key")).to eql("stored_value")
        end
      end
    end

    context "when table does not exist" do
      it "raises TableDoesNotExistException" do
        with_keyspace("keyspace_name") do
          table_missing_exception = [
            TableDoesNotExistException,
            %{Table "non_existent_table" does not exist}
          ]

          expect {
            client.store(keyspace_name: "keyspace_name", table_name: "non_existent_table", key: "stored_key", value: "stored_value")
          }.to raise_exception(*table_missing_exception)

          expect {
            client.fetch(keyspace_name: "keyspace_name", table_name: "non_existent_table", key: "stored_key")
          }.to raise_exception(*table_missing_exception)
        end
      end
    end

    context "when key does not exist" do
      it "raises KeyNotFoundException" do
        with_keyspace("keyspace_name") do
          with_table("keyspace_name", "table") do
            expect {
              client.fetch(keyspace_name: "keyspace_name", table_name: "table", key: "non_existent_key")
            }.to raise_exception(KeyNotFoundException, %{"non_existent_key" key not found})
          end
        end
      end
    end
  end

  describe "#session" do
    it "memoizes" do
      expect(client.session).to eql(client.session)
    end

    context "when all the connection details are correct" do
      it "connects to Cassandra" do
        expect(client).to be_connected
      end
    end

    context "when connection details are not correct" do
      it "raises InvalidCassandraCredentialsException" do
        incorrect_credentials = {
          'node_ips'      => node_ips,
          "username"      => "non_existent",
          "password"      => "invalid",
        }

        expect {
          CassandraClient.new(connection_details: incorrect_credentials).session
        }.to raise_exception(InvalidCassandraCredentialsException)
      end
    end

    context "when Cassandra is not available" do
      it "raises CassandraUnavailableException" do
        incorrect_cassandra_host = {
          'node_ips'            => %w[127.0.0.2],
          "username"            => "cassandra",
          "password"            => "cassandra",
          "connection_timeout"  => 0.1
        }

        expect {
          CassandraClient.new(connection_details: incorrect_cassandra_host).session
        }.to raise_exception(CassandraUnavailableException)
      end
    end
  end
end
