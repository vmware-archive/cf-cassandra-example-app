require 'cassandra'
require 'forwardable'

InvalidCassandraCredentialsException = Class.new(Exception)
CassandraUnavailableException = Class.new(Exception)
InvalidTableName = Class.new(Exception)
InvalidKeyspaceName = Class.new(Exception)
TableDoesNotExistException = Class.new(Exception)
KeyNotFoundException = Class.new(Exception)

class CassandraClient < SimpleDelegator
  def initialize(args)
    @connection_details = args.fetch(:connection_details)
    super(session)
  end

  def cluster
    @cluster = Cassandra.cluster(remapped_connection_details)
  end

  def connected?
    @session != nil
  end

  def session
    @session ||= cluster.connect

  rescue  Cassandra::Errors::AuthenticationError => exception
     raise(InvalidCassandraCredentialsException, exception)
  rescue Cassandra::Errors::NoHostsAvailable => exception
     raise(CassandraUnavailableException, exception)
  end


  def keyspace_exists?(keyspace_name)
    cluster.has_keyspace? keyspace_name
  end

  def table_exists?(keyspace_name, table_name)
    query = %{
      SELECT table_name
      FROM system_schema.tables
      WHERE keyspace_name=? AND table_name=?
    }

    prepared_statement = session.prepare(query)
    result = session.execute(prepared_statement, {arguments: [keyspace_name, table_name]})
    result.one?
  end

  def create_keyspace(keyspace_name)
    raise InvalidKeyspaceName if keyspace_name.index(/[^0-9a-z_]/i)

    return if keyspace_exists?(keyspace_name)

    query = %{
      CREATE KEYSPACE "#{keyspace_name}"
      WITH REPLICATION = {
        'class' : 'SimpleStrategy',
        'replication_factor' : 1
      }
    }

    session.execute(query)
  end

  def create_table(keyspace_name, table_name)
    raise InvalidTableName if table_name.index(/[^0-9a-z_]/i)

    return if table_exists?(keyspace_name, table_name)

    query = %{
      CREATE TABLE "#{keyspace_name}"."#{table_name}" (
        id varchar PRIMARY KEY,
        value varchar
      )
    }

    session.execute(query)
  end

  def delete_keyspace(keyspace_name)
    return unless keyspace_exists?(keyspace_name)

    query = %{
      DROP KEYSPACE "#{keyspace_name}"
    }

    session.execute(query)
  end

  def store(args)
    keyspace_name = args.fetch(:keyspace_name)
    table_name = args.fetch(:table_name)
    key = args.fetch(:key)
    value = args.fetch(:value)

    ensure_table_exists(keyspace_name, table_name)

    query = %{
      INSERT INTO "#{keyspace_name}"."#{table_name}" (id, value)
      VALUES (?, ?)
    }

    statement = session.prepare(query)
    session.execute(statement, {arguments: [key, value]})
  end

  def fetch(args)
    keyspace_name = args.fetch(:keyspace_name)
    table_name = args.fetch(:table_name)
    key = args.fetch(:key)

    ensure_table_exists(keyspace_name, table_name)

    query = %{
      SELECT value
      FROM  "#{keyspace_name}"."#{table_name}"
      WHERE id=?
    }

    statement = session.prepare(query)
    result = session.execute(statement, {arguments: [key]})

    raise(KeyNotFoundException, %{"#{key}" key not found}) unless result.first

    result.first.fetch("value")
  end

  private

  attr_reader :connection_details

  def ensure_table_exists(keyspace_name, table_name)
    unless table_exists?(keyspace_name, table_name)
      raise(TableDoesNotExistException, %{Table "#{table_name}" does not exist})
    end
  end

  def keyspace_name
    connection_details.fetch('keyspace_name')
  end

  def username
    connection_details.fetch('username', "cassandra")
  end

  def password
    connection_details.fetch('password', "cassandra")
  end

  def hosts
    connection_details.fetch('node_ips', %w[localhost])
  end

  def connection_timeout
    connection_details.fetch('connection_timeout', 10).to_i
  end

  def remapped_connection_details
    {

        username: username,
        password: password,
      hosts: hosts,
      connection_timeout: connection_timeout,
    }
  end
end
