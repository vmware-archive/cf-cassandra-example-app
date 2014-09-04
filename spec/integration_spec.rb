require "spec_helper"
require "support/cassandra_example_app"

require "faraday"

describe "Cassandra Example app" do
  let(:app_client) { Faraday.new("http://localhost:9292/") }

  def app_lifecycle(app: example_app, teardown: [])
    begin
      yield
    ensure
      teardown_steps = Array(teardown)
      teardown_steps.each(&:call)
      app.stop
    end
  end

  context "when the application doesn't have the Cassandra connection details (i.e. not bound/available)" do
    it "app starts fine and returns error instructing the user on how to bind the app to Cassandra" do
      app_lifecycle(app: CassandraExampleApp.start_without_cassandra_binding) do
        response = app_client.post("/table")
        expect(response.status).to eql(500)
        expect(response.body).to include("cf bind-service app-name cassandra-instance")
      end
    end
  end

  context "when the application has the Cassandra connection details" do
    it "can read and write data to Cassandra" do
      app_lifecycle(app: CassandraExampleApp.start_with_cassandra_binding) do
        app_client.post("/table")
        request = app_client.post("/table/key/value")
        expect(request.status).to eql(201)
      end

      # Restart the app so that we ensure data was actually persisted

      app_lifecycle(app: CassandraExampleApp.start_with_cassandra_binding, teardown: ->{ app_client.delete("/table") }) do
        response = app_client.get("/table/key")
        expect(response.status).to eql(200)
        expect(response.body).to eql("value")
      end
    end

    it "if the CassandraClient raises an exception, the app displays the exception message" do
      app_lifecycle(app: CassandraExampleApp.start_with_cassandra_binding, teardown: ->{ app_client.delete("/table") }) do
        app_client.post("/table")
        response = app_client.get("/table/inexistent_key")
        expect(response.status).to eql(500)
        expect(response.body).to include(%{"inexistent_key" key not found})
      end
    end
  end
end
