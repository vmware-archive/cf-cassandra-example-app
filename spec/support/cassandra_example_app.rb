require "faraday"
require "json"
require "childprocess"

class CassandraExampleApp
  AppNotStartedException = Class.new(Exception)

  attr_reader :vcap_services, :process

  def self.start_with_cassandra_binding
    new(vcap_services: {
      "p-cassandra-dev" => [
        {
          "name" => "cassie",
          "label" => "p-cassandra-dev",
          "tags" => %w[cassandra pivotal],
          "plan" => "default",
          "credentials" => {
            "node_ips" => %w[127.0.0.1],
            "thrift_port" => 9160,
            "cql_port" => 9042,
            "keyspace_name" => "rspec_cf_cassandra_example_app",
            "username" => "cassandra",
            "password" => "cassandra"
          }
        }
      ]
    }).start
  end

  def self.start_without_cassandra_binding
    new.start
  end

  def stop
    process.stop
    process.poll_for_exit(5)
  end

  def started?
    Faraday.get("http://localhost:9292/")
  rescue Faraday::ConnectionFailed => e
    false
  end

  def start
    @process ||= begin
      app = ChildProcess.build("rackup")
      app.environment["VCAP_SERVICES"] = vcap_services.to_json
      app.environment["RACK_ENV"] = "production"
      app.start
    end

    fail_if_not_started_within_seconds(5)

    self
  end

  private

  def initialize(args = {})
    @vcap_services = args.fetch(:vcap_services, {})
  end

  def fail_if_not_started_within_seconds(seconds)
    process.send(:log, "Waiting #{seconds} seconds to start")
    end_time = Time.now + seconds

    until process.alive? && started?
      sleep 0.1
      if Time.now > end_time
        raise TimeoutError, "Not started after #{seconds} seconds"
      end
    end
  end
end
