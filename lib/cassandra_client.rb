require 'cql'
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
    super(client)
  end

  def client
    @client ||= Cql::Client.connect(remapped_connection_details)
  rescue Cql::AuthenticationError => exception
    raise(InvalidCassandraCredentialsException, exception)
  rescue Cql::Io::ConnectionTimeoutError => exception
    raise(CassandraUnavailableException, exception)
  end

  def keyspace_exists?(keyspace_name)
    query = %{
      SELECT *
      FROM system.schema_keyspaces
      WHERE keyspace_name=?
    }

    result = client.prepare(query).execute(keyspace_name)
    result.one?
  end

  def table_exists?(keyspace_name, table_name)
    client.use(keyspace_name)
    query = %{
      SELECT columnfamily_name
      FROM system.schema_columnfamilies
      WHERE keyspace_name=? AND columnfamily_name=?
    }

    result = client.prepare(query).execute(keyspace_name, table_name)
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

    client.execute(query)
  end

  def create_table(keyspace_name, table_name)
    raise InvalidTableName if table_name.index(/[^0-9a-z_]/i)
    client.use(keyspace_name)

    return if table_exists?(keyspace_name, table_name)

    query = %{
      CREATE TABLE "#{table_name}" (
        id varchar PRIMARY KEY,
        value varchar
      )
    }

    client.execute(query)
  end

  def delete_keyspace(keyspace_name)
    return unless keyspace_exists?(keyspace_name)

    query = %{
      DROP KEYSPACE "#{keyspace_name}"
    }

    client.execute(query)
  end

  def store(args)
    keyspace_name = args.fetch(:keyspace_name)
    table_name = args.fetch(:table_name)
    key = args.fetch(:key)
    value = args.fetch(:value)

    ensure_table_exists(keyspace_name, table_name)

    query = %{
      INSERT INTO "#{table_name}" (id, value)
      VALUES (?, ?)
    }

    client.use(keyspace_name)
    client.prepare(query).execute(key, value)
  end

  def fetch(args)
    keyspace_name = args.fetch(:keyspace_name)
    table_name = args.fetch(:table_name)
    key = args.fetch(:key)

    ensure_table_exists(keyspace_name, table_name)

    query = %{
      SELECT value
      FROM "#{table_name}"
      WHERE id=?
    }

    client.use(keyspace_name)
    result = client.prepare(query).execute(key)

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
      credentials: {
        username: username,
        password: password,
      },
      hosts: hosts,
      connection_timeout: connection_timeout,
    }
  end
end
