require "cassandra"

module CassandraTestEnvironment
  extend self

  def session
    @session ||= Cassandra.cluster(username: "cassandra", password: "cassandra",
                                    hosts: node_ips).connect
  rescue Cassandra::Errors::NoHostsAvailable
     puts "Cannot connect to Cassandra, is the Cassandra Server running?"
     exit 1
  end

  def setup
    create_keyspace
  end

  def teardown
    delete_keyspace
  end

  def create_keyspace
    session.execute("CREATE KEYSPACE IF NOT EXISTS #{keyspace} WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}")
  end

  def delete_keyspace
    session.execute("DROP KEYSPACE IF EXISTS #{keyspace}")
  rescue
  end

  def keyspace
    "rspec_cf_cassandra_example_app"
  end
end
