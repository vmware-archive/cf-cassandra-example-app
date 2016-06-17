require "pry"
require "securerandom"

Dir.glob(File.expand_path('../support/*.rb', __FILE__), &method(:require))

RSpec.configure do |config|
  config.run_all_when_everything_filtered = true
  config.filter_run :focus
  config.order = 'random'
  config.before(:suite) { CassandraTestEnvironment.setup }
  config.after(:suite) { CassandraTestEnvironment.teardown }
end

def node_ips
  nodes = ENV["NODE_IPS"] || "localhost"
  @node_ips ||= nodes.split(",")
end
