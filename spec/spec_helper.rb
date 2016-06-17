require "pry"
require "securerandom"
require "yaml"

Dir.glob(File.expand_path('../support/*.rb', __FILE__), &method(:require))

RSpec.configure do |config|
  config.run_all_when_everything_filtered = true
  config.filter_run :focus
  config.order = 'random'
  config.before(:suite) { CassandraTestEnvironment.setup }
  config.after(:suite) { CassandraTestEnvironment.teardown }
end

def node_ips
  @node_ips ||= YAML.load_file('spec/node_ips.yml')['node_ips']
end
