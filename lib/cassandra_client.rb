require 'cql'
require 'forwardable'

InvalidCassandraCredentialsException = Class.new(Exception)
CassandraUnavailableException = Class.new(Exception)
InvalidTableName = Class.new(Exception)
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

  def table_exists?(table_name)
    query = %{
      SELECT columnfamily_name
      FROM system.schema_columnfamilies
      WHERE keyspace_name=? AND columnfamily_name=?
    }

    result = client.prepare(query).execute(keyspace_name, table_name)
    result.one?
  end

  def create_table(table_name)
    raise InvalidTableName if table_name.index(/[^0-9a-z_]/i)

    return if table_exists?(table_name)

    query = %{
      CREATE TABLE "#{table_name}" (
        id varchar PRIMARY KEY,
        value varchar
      )
    }

    client.execute(query)
  end

  def delete_table(table_name)
    return unless table_exists?(table_name)

    query = %{
      DROP TABLE "#{table_name}"
    }

    client.execute(query)
  end

  def store(args)
    table_name = args.fetch(:table_name)
    key = args.fetch(:key)
    value = args.fetch(:value)

    ensure_table_exists(table_name)

    query = %{
      INSERT INTO "#{table_name}" (id, value)
      VALUES (?, ?)
    }

    client.prepare(query).execute(key, value)
  end

  def fetch(args)
    table_name = args.fetch(:table_name)
    key = args.fetch(:key)

    ensure_table_exists(table_name)

    query = %{
      SELECT value
      FROM "#{table_name}"
      WHERE id=?
    }

    result = client.prepare(query).execute(key)

    raise(KeyNotFoundException, %{"#{key}" key not found}) unless result.first

    result.first.fetch("value")
  end

  private

  attr_reader :connection_details

  def ensure_table_exists(table_name)
    unless table_exists?(table_name)
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
      keyspace: keyspace_name,
      credentials: {
        username: username,
        password: password,
      },
      hosts: hosts,
      connection_timeout: connection_timeout,
    }
  end
end
