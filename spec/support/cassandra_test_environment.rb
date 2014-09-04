require "cql"

module CassandraTestEnvironment
  extend self

  def client
    @client ||= Cql::Client.connect(credentials: { username: "cassandra", password: "cassandra" })
  rescue Cql::Io::ConnectionError
    puts "Cannot connect to Cassandra, is the Cassandra Server running?"
    exit 1
  end

  def setup
    create_keyspace unless keyspace_present?
  end

  def teardown
    delete_keyspace
  end

  def create_keyspace
    client.execute("CREATE KEYSPACE #{keyspace} WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}")
  end

  def delete_keyspace
    client.execute("DROP KEYSPACE #{keyspace}")
  end

  def keyspace
    "rspec_cf_cassandra_example_app"
  end

  def keyspace_present?
    client.execute("SELECT keyspace_name FROM system.schema_keyspaces WHERE keyspace_name = '#{keyspace}'").one?
  end
end

