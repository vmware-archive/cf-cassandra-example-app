# Example Cassandra Application

This simple application illustrates the use of the Pivotal Cassandra data service in a Ruby application running on Pivotal Cloud Foundry.

## Installation

#### Create a Cassandra service instance

Find your Cassandra service via `cf marketplace`.

```
$ cf marketplace
Getting services from marketplace in org testing / space testing as me...
OK

service       plans     description
p-cassandra   default   Cassandra service
```

Our service is called `p-cassandra`.  To create an instance of this service, use:

```
$ cf create-service p-cassandra default cassandra
```

#### Push the Example Application

The example application comes with a Cloud Foundry `manifest.yml` file, which provides all of the defaults necessary for an easy `cf push`.

```
$ cf push
Using manifest file cf-cassandra-example-app/manifest.yml

Creating app cassandra-example-app in org testing / space testing as me...
OK

Using route cassandra-example-app.example.com
Binding cassandra-example-app.example.com to cassandra-example-app...
OK

Uploading cassandra-example-app...
Uploading from: cf-cassandra-example-app
...
Showing health and status for app cassandra-example-app in org testing / space testing as me...
OK

requested state: started
instances: 0/1
usage: 256M x 1 instances
urls: cassandra-example-app.10.244.0.34.xip.io

     state     since                    cpu    memory          disk
#0   running   2014-04-10 01:42:43 PM   0.0%   75.5M of 256M   0 of 1G
```

If you now curl the application, you'll see that the application has detected that it's not bound to a cassandra instance.

```
$ curl http://cassandra-example-app.example.com/

      You must bind a Cassandra service instance to this application.

      You can run the following commands to create an instance and bind to it:

        $ cf create-service cassandra default cassandra-instance
        $ cf bind-service app-name cassandra-instance
```

#### Bind the Instance

Now, simply bind the cassandra instance to our application.

```
$ cf bind-service cassandra-example-app cassandra
```

## Usage

You can now read and write records by GETting and POSTing to `/table/key`.  Be sure to create the table, first.  In the example below, we create a table named `entries`, add a key/value pair named `foo` with a value of `bar`, and retrieve the value back from `foo`.

```
$ curl -X POST http://cassandra-example-app.example.com/entries
$ curl -X POST http://cassandra-example-app.example.com/entries/foo/bar
$ curl -X GET  http://cassandra-example-app.example.com/entries/foo
bar
```

Of course, be sure to replace `example.com` with the actual domain of your Pivotal Cloud Foundry installation.
