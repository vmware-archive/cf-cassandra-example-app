require 'sinatra'

class CassandraExampleApp < Sinatra::Base
  before do
    content_type "text/plain"

    if cassandra_service_not_bound_to_app?
      halt(500, bind_cassandra_service_to_app_instructions)
    end
  end

  error do |exception|
    halt(500, exception.message)
  end

  post '/:keyspace_name' do
    cassandra_client.create_keyspace(params[:keyspace_name])
  end

  post '/:keyspace_name/:table_name' do
    cassandra_client.create_table(params[:keyspace_name], params[:table_name])
  end

  delete '/:keyspace_name' do
    cassandra_client.delete_keyspace(params[:keyspace_name])
  end

  post '/:keyspace_name/:table_name/:key/:value' do
    status 201
    cassandra_client.store(
      keyspace_name: params[:keyspace_name],
      table_name: params[:table_name],
      key: params[:key],
      value: params[:value]
    )
  end

  get '/:keyspace_name/:table_name/:key' do
    cassandra_client.fetch(
      keyspace_name: params[:keyspace_name],
      table_name: params[:table_name],
      key: params[:key]
    )
  end

  private

  def bind_cassandra_service_to_app_instructions
    %{
      You must bind a Cassandra service instance to this application.

      You can run the following commands to create an instance and bind to it:

        $ cf create-service cassandra default cassandra-instance
        $ cf bind-service app-name cassandra-instance
    }
  end

  def cassandra_client
    @cassandra_client ||= begin
      require 'cassandra_client'
      CassandraClient.new(connection_details: cassandra_connection_details)
    end
  end

  def cassandra_connection_details
    @cassandra_connection_details ||= begin
      require "cf-app-utils"
      CF::App::Credentials.find_all_by_all_service_tags(%w[cassandra pivotal]).first
    end
  end

  def cassandra_service_not_bound_to_app?
    !cassandra_connection_details
  end
end
