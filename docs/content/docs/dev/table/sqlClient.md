---
title: "SQL Client"
weight: 91
type: docs
aliases:
  - /dev/table/sqlClient.html
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# SQL Client


Flink’s Table & SQL API makes it possible to work with queries written in the SQL language, but these queries need to be embedded within a table program that is written in either Java or Scala. Moreover, these programs need to be packaged with a build tool before being submitted to a cluster. This more or less limits the usage of Flink to Java/Scala programmers.

The *SQL Client* aims to provide an easy way of writing, debugging, and submitting table programs to a Flink cluster without a single line of Java or Scala code. The *SQL Client CLI* allows for retrieving and visualizing real-time results from the running distributed application on the command line.

<a href="/fig/sql_client_demo.gif"><img class="offset" src="/fig/sql_client_demo.gif" alt="Animated demo of the Flink SQL Client CLI running table programs on a cluster" width="80%" /></a>



Getting Started
---------------

This section describes how to setup and run your first Flink SQL program from the command-line.

The SQL Client is bundled in the regular Flink distribution and thus runnable out-of-the-box. It requires only a running Flink cluster where table programs can be executed. For more information about setting up a Flink cluster see the [Cluster & Deployment]({{< ref "docs/deployment/resource-providers/standalone/overview" >}}) part. If you simply want to try out the SQL Client, you can also start a local cluster with one worker using the following command:

```bash
./bin/start-cluster.sh
```

### Starting the SQL Client CLI

The SQL Client scripts are also located in the binary directory of Flink. [In the future](sqlClient.html#limitations--future), a user will have two possibilities of starting the SQL Client CLI either by starting an embedded standalone process or by connecting to a remote SQL Client Gateway. At the moment only the `embedded` mode is supported, and default mode is `embedded`. You can start the CLI by calling:

```bash
./bin/sql-client.sh
```

or explicitly use `embedded` mode:

```bash
./bin/sql-client.sh embedded
```

### Running SQL Queries

Once the CLI has been started, you can use the `HELP` command to list all available SQL statements. For validating your setup and cluster connection, you can enter your first SQL query and press the `Enter` key to execute it:

```sql
SELECT 'Hello World';
```

This query requires no table source and produces a single row result. The CLI will retrieve results from the cluster and visualize them. You can close the result view by pressing the `Q` key.

The CLI supports **three modes** for maintaining and visualizing results.

The **table mode** materializes results in memory and visualizes them in a regular, paginated table representation. It can be enabled by executing the following command in the CLI:

```text
SET sql-client.execution.result-mode=table;
```

The **changelog mode** does not materialize results and visualizes the result stream that is produced by a [continuous query](streaming/dynamic_tables.html#continuous-queries) consisting of insertions (`+`) and retractions (`-`).

```text
SET sql-client.execution.result-mode=changelog;
```

The **tableau mode** is more like a traditional way which will display the results in the screen directly with a tableau format.
The displaying content will be influenced by the query execution type(`execution.type`).

```text
SET sql-client.execution.result-mode=tableau;
```

Note that when you use this mode with streaming query, the result will be continuously printed on the console. If the input data of
this query is bounded, the job will terminate after Flink processed all input data, and the printing will also be stopped automatically.
Otherwise, if you want to terminate a running query, just type `CTRL-C` in this case, the job and the printing will be stopped.

You can use the following query to see all the result modes in action:

```sql
SELECT name, COUNT(*) AS cnt FROM (VALUES ('Bob'), ('Alice'), ('Greg'), ('Bob')) AS NameTable(name) GROUP BY name;
```

This query performs a bounded word count example.

In *changelog mode*, the visualized changelog should be similar to:

```text
+ Bob, 1
+ Alice, 1
+ Greg, 1
- Bob, 1
+ Bob, 2
```

In *table mode*, the visualized result table is continuously updated until the table program ends with:

```text
Bob, 2
Alice, 1
Greg, 1
```

In *tableau mode*, if you ran the query in streaming mode, the displayed result would be:
```text
+-----+----------------------+----------------------+
| +/- |                 name |                  cnt |
+-----+----------------------+----------------------+
|   + |                  Bob |                    1 |
|   + |                Alice |                    1 |
|   + |                 Greg |                    1 |
|   - |                  Bob |                    1 |
|   + |                  Bob |                    2 |
+-----+----------------------+----------------------+
Received a total of 5 rows
```

And if you ran the query in batch mode, the displayed result would be:
```text
+-------+-----+
|  name | cnt |
+-------+-----+
| Alice |   1 |
|   Bob |   2 |
|  Greg |   1 |
+-------+-----+
3 rows in set
```

All these result modes can be useful during the prototyping of SQL queries. In all these modes, results are stored in the Java heap memory of the SQL Client. In order to keep the CLI interface responsive, the changelog mode only shows the latest 1000 changes. The table mode allows for navigating through bigger results that are only limited by the available main memory and the configured [maximum number of rows](sqlClient.html#configuration) (`max-table-result-rows`).

<span class="label label-danger">Attention</span> Queries that are executed in a batch environment, can only be retrieved using the `table` or `tableau` result mode.

After a query is defined, it can be submitted to the cluster as a long-running, detached Flink job. For this, a target system that stores the results needs to be specified using the [INSERT INTO statement](sqlClient.html#detached-sql-queries). The [configuration section](sqlClient.html#configuration) explains how to declare table sources for reading data, how to declare table sinks for writing data, and how to configure other table program properties.

{{< top >}}

Configuration
-------------

### SQL Client startup options

The SQL Client can be started with the following optional CLI commands. They are discussed in detail in the subsequent paragraphs.

```text
./bin/sql-client.sh --help

Mode "embedded" (default) submits Flink jobs from the local machine.

  Syntax: [embedded] [OPTIONS]
  "embedded" mode options:
         -d,--defaults <environment file>      Deprecated feature: the environment
                                               properties with which every new
                                               session is initialized. Properties
                                               might be overwritten by session
                                               properties.
         -e,--environment <environment file>   Deprecated feature: the environment
                                               properties to be imported into the
                                               session. It might overwrite default
                                               environment properties.
         -f,--file <script file>               Script file that should be executed.
                                               In this mode, the client will not
                                               open an interactive terminal.
         -h,--help                             Show the help message with
                                               descriptions of all options.
         -hist,--history <History file path>   The file which you want to save the
                                               command history into. If not
                                               specified, we will auto-generate one
                                               under your user's home directory.
         -i,--init <initialization file>       Script file that used to init the
                                               session context. If get error in
                                               execution, the sql client will exit.
                                               Notice it's not allowed to add query
                                               or insert into the init file.
         -j,--jar <JAR file>                   A JAR file to be imported into the
                                               session. The file might contain
                                               user-defined classes needed for the
                                               execution of statements such as
                                               functions, table sources, or sinks.
                                               Can be used multiple times.
         -l,--library <JAR directory>          A JAR file directory with which every
                                               new session is initialized. The files
                                               might contain user-defined classes
                                               needed for the execution of
                                               statements such as functions, table
                                               sources, or sinks. Can be used
                                               multiple times.
         -pyarch,--pyArchives <arg>            Add python archive files for job. The
                                               archive files will be extracted to
                                               the working directory of python UDF
                                               worker. Currently only zip-format is
                                               supported. For each archive file, a
                                               target directory be specified. If the
                                               target directory name is specified,
                                               the archive file will be extracted to
                                               a name can directory with the
                                               specified name. Otherwise, the
                                               archive file will be extracted to a
                                               directory with the same name of the
                                               archive file. The files uploaded via
                                               this option are accessible via
                                               relative path. '#' could be used as
                                               the separator of the archive file
                                               path and the target directory name.
                                               Comma (',') could be used as the
                                               separator to specify multiple archive
                                               files. This option can be used to
                                               upload the virtual environment, the
                                               data files used in Python UDF (e.g.:
                                               --pyArchives
                                               file:///tmp/py37.zip,file:///tmp/data
                                               .zip#data --pyExecutable
                                               py37.zip/py37/bin/python). The data
                                               files could be accessed in Python
                                               UDF, e.g.: f = open('data/data.txt',
                                               'r').
         -pyexec,--pyExecutable <arg>          Specify the path of the python
                                               interpreter used to execute the
                                               python UDF worker (e.g.:
                                               --pyExecutable
                                               /usr/local/bin/python3). The python
                                               UDF worker depends on Python 3.6+,
                                               Apache Beam (version == 2.27.0), Pip
                                               (version >= 7.1.0) and SetupTools
                                               (version >= 37.0.0). Please ensure
                                               that the specified environment meets
                                               the above requirements.
         -pyfs,--pyFiles <pythonFiles>         Attach custom python files for job.
                                               The standard python resource file
                                               suffixes such as .py/.egg/.zip or
                                               directory are all supported. These
                                               files will be added to the PYTHONPATH
                                               of both the local client and the
                                               remote python UDF worker. Files
                                               suffixed with .zip will be extracted
                                               and added to PYTHONPATH. Comma (',')
                                               could be used as the separator to
                                               specify multiple files (e.g.:
                                               --pyFiles
                                               file:///tmp/myresource.zip,hdfs:///$n
                                               amenode_address/myresource2.zip).
         -pyreq,--pyRequirements <arg>         Specify a requirements.txt file which
                                               defines the third-party dependencies.
                                               These dependencies will be installed
                                               and added to the PYTHONPATH of the
                                               python UDF worker. A directory which
                                               contains the installation packages of
                                               these dependencies could be specified
                                               optionally. Use '#' as the separator
                                               if the optional parameter exists
                                               (e.g.: --pyRequirements
                                               file:///tmp/requirements.txt#file:///
                                               tmp/cached_dir).
         -s,--session <session identifier>     The identifier for a session.
                                               'default' is the default identifier.
         -u,--update <SQL update statement>    Deprecated Experimental (for testing
                                               only!) feature: Instructs the SQL
                                               Client to immediately execute the
                                               given update statement after starting
                                               up. The process is shut down after
                                               the statement has been submitted to
                                               the cluster and returns an
                                               appropriate return code. Currently,
                                               this feature is only supported for
                                               INSERT INTO statements that declare
                                               the target sink table.Please use
                                               option -f to submit update statement.
```

### SQL Client Options

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left" style="width: 25%">Option</th>
        <th class="text-center" style="width: 8%">Required</th>
        <th class="text-center" style="width: 7%">Default</th>
        <th class="text-center" style="width: 10%">Type</th>
        <th class="text-center" style="width: 50%">Description</th>
      </tr>
    </thead>
    <tr>
      <td><h5>sql-client.execution.max-table-result.rows</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1000_000</td>
      <td>Integer</td>
      <td>The number of rows to cache when in the table mode. If the number of rows exceeds the specified value, it retries the row in the FIFO style.</td>
    </tr>
    <tr>
      <td><h5>sql-client.execution.result-mode</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">table</td>
      <td>String</td>
      <td>Determine the mode when display the query result. The available values are ['table', 'tableau', 'changelog'].
      The 'table' mode materializes results in memory and visualizes them in a regular, paginated table representation.
      The 'changelog' mode does not materialize results and visualizes the result stream that is produced by a continuous query.
      The 'tableau' mode is more like a traditional way which will display the results in the screen directly with a tableau format.
      </td>
    </tr>
    <tr>
      <td><h5>sql-client.verbose</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>Determine whether to output the verbose output to the console. If set the option true, it will print the exception stack. Otherwise, it only output the cause.</td>
    </tr>
</table>

### Use SQL Files to initialize session

A SQL query needs a configuration environment in which it is executed. Using -i option to import the SQL file to initialize the environment when start up.

An example of such a file is presented below.

```sql
-- Define available catalogs

CREATE CATALOG MyCatalog
  WITH (
    'type' = 'hive'
  );

USE MyCatalog;

-- Define available database

CREATE DATABASE MyDatabase;

USE MyDatabase;

-- Define TABLE

CREATE TABLE MyTable(
  MyField1 INT,
  MyField2 STRING
) WITH (
  'connector' = 'filesystem',
  'path' = '/path/to/something',
  'format' = 'csv'
);

-- Define VIEW

CREATE VIEW MyCustomView AS SELECT MyField2 FROM MyTable;

-- Define user-defined functions here.

CREATE FUNCTION foo.bar.AggregateUDF AS myUDF;

-- Properties that change the fundamental execution behavior of a table program.

SET table.planner = blink; -- planner: either 'blink' (default) or 'old'
SET execution.runtime-mode = streaming; -- execution mode either 'batch' or 'streaming'
SET sql-client.execution.result-mode = table; -- available values: 'table', 'changelog' and 'tableau'
SET sql-client.execution.max-table-result.rows = 10000; -- optional: maximum number of maintained rows
SET parallelism.default = 1; -- optional: Flink's parallelism (1 by default)
SET pipeline.auto-watermark-interval = 200; --optional: interval for periodic watermarks
SET pipeline.max-parallelism = 10; -- optional: Flink's maximum parallelism
SET table.exec.state.ttl=1000; -- optional: table program's idle state time
SET restart-strategy = fixed-delay;

-- Configuration options for adjusting and tuning table programs.

SET table.optimizer.join-reorder-enabled = true;
SET table.exec.spill-compression.enabled = true;
SET table.exec.spill-compression.block-size = 128kb;
```

This configuration:

- connects to Hive catalogs and uses `MyCatalog` as the current catalog with `MyDatabase` as the current database of the catalog,
- defines a table `MyTableSource` that can read data from a CSV file,
- defines a view `MyCustomView` that declares a virtual table using a SQL query,
- defines a user-defined function `myUDF` that can be instantiated using the class name,
- uses the blink planner in streaming mode for running statements and a parallelism of 1,
- runs exploratory queries in the `table` result mode,
- and makes some planner adjustments around join reordering and spilling via configuration options.

When using `-i <init.sql>` option to initialize SQL Client session, the following statements are not allowed in an initialization SQL file:
 - INSERT INTO/OVERWRITE
 - STATEMENT SET
 - SELECT queries
- DDL(CREATE/DROP/ALTER)
- USE CATALOG/DATABASE
- LOAD/UNLOAD MODULE
- SET command
- RESET command

to build the execution environment.

When execute queries or insert statement, please enter the interactive mode or use the -f option to submit the SQL statements.

<span class="label label-danger">Attention</span> If SQL Client meets errors when execute initialization file, SQL Client will exit with error messages.

Use SQL Client to submit job
----------------------------

SQL Client allows users to submit jobs either within the interactive command line or using `-f` option to execute sql file.

In both modes, SQL Client supports to parse and execute all types of the Flink supported SQL statements.

### Interactive Mode

In interactive mode, the SQL Client reads user inputs and executes the statement when meeting semicolon (`;`).


In interactive mode, SQL Client will print success message when statement is executed successfully. On the other hand, SQL Client will print error message if the execution is failed. By default, the error message only contains a cause. In order to print the full exception stack for debugging, please set the `sql-client.verbose` to true through `SET sql-client.verbose = true;`.

### Execute SQL Files

SQL Client supports to execute a SQL script file with the `-f` option. SQL Client will execute statements one by one in the SQL script file and print execution messages for each executed statements. Once a statement is failed, the SQL Client will exist and all the remaining statements will not be executed. 
An example of such a file is presented below.

```sql
CREATE TEMPORARY TABLE users (
  user_id BIGINT,
  user_name STRING,
  user_level STRING,
  region STRING,
  PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'users',
  'properties.bootstrap.servers' = '...',
  'key.format' = 'csv',
  'value.format' = 'avro'
);

-- set sync mode
SET table.dml-sync=true;

-- set the job name
SET pipeline.name=SqlJob;

-- set the queue that the job submit to
SET yarn.application.queue=root;

-- set the job parallism
SET parallism.default=100;

-- restore from the specific savepoint path
SET execution.savepoint.path=/tmp/flink-savepoints/savepoint-cca7bc-bb1e257f0dab;

INSERT INTO pageviews_enriched
SELECT *
FROM pageviews AS p
LEFT JOIN users FOR SYSTEM_TIME AS OF p.proctime AS u
ON p.user_id = u.user_id;
```
This configuration:

- defines a temporal table source `users` that reads from a CSV file,
- set the properties, e.g job name,
- set the savepoint path,
- submit a sql job that load the savepoint from the specified savepoint path.

<span class="label label-danger">Attention</span> Comparing to interactive mode, SQL Client will stop execution and exits when meets errors.

Build pipelines with SQL Client
-----------------------------

By default the SQL Client submit the `INSERT INTO` statement to a Flink cluster in detached mode. These queries produce their results into an external system instead of the SQL Client. This allows for dealing with higher parallelism and larger amounts of data. The CLI itself does not have any control over a detached query after submission.

```sql
INSERT INTO MyTableSink SELECT * FROM MyTableSource
```

The SQL Client makes sure that a statement is successfully submitted to the cluster. Once the query is submitted, the CLI will show information about the Flink job.

```text
[INFO] Table update statement has been successfully submitted to the cluster:
Cluster ID: StandaloneClusterId
Job ID: 6f922fe5cba87406ff23ae4a7bb79044
```

<span class="label label-danger">Attention</span> The SQL Client does not track the status of the running Flink job after submission. The CLI process can be shutdown after the submission without affecting the detached query. Flink's `restart strategy` takes care of the fault-tolerance. A query can be cancelled using Flink's web interface, command-line, or REST API.

### Execute a batch of SQL statements

Sometimes it's prefer to batch insert statements and optimize/execute the batched statements together. In SQL Client, it supports to parse and execute statement set in SQL.

```text
Flink SQL> BEGIN STATEMENT SET;
[INFO] Begin a statement set.

Flink SQL> INSERT INTO MySink1 SELECT * FROM MySource1;
[INFO] Add SQL update statement to the statement set.

Flink SQL> INSERT INTO MySink2 SELECT * FROM MySource1;
[INFO] Add SQL update statement to the statement set.

Flink SQL> END;
[INFO] Submitting SQL update statement to the cluster...
[INFO] Execute statement in sync mode. Please wait for the execution finish...
[INFO] Complete execution of the SQL update statement.
```

<span class="label label-danger">Attention</span> It's allowed to add `INSERT` statements into the statement set only.

### Execute DML Statements one by one

Sometimes it's better to execute statements one by one. In SQL Client, it can use the option `table.dml-sync` to wait for the job finishes.

```text
Flink SQL> SET table.dml-sync = true;
[INFO] Session property has been set.

Flink SQL> INSERT INTO MyTableSink SELECT * FROM MyTableSource;
[INFO] Submitting SQL update statement to the cluster...
[INFO] Execute statement in sync mode. Please wait for the execution finish...
[INFO] Complete execution of the SQL update statement.
```

<span class="label label-danger">Attention</span> It's supported to cancel the running job by CTRL + C;

{{< top >}}

Compatibility
-------------

To be compatible with before, SQL Client still supports to initialize with environment YAML file and allows to SET the key in YAML file.
When set the key defined in YAML file, the SQL Client will print the warning messages to inform.

```text
Flink SQL> SET execution.type = batch;
[WARNING] The specified key 'execution.type' is deprecated. Please use 'execution.runtime-mode' instead.
[INFO] Session property has been set.
```

When using `SET` command to print the properties, the SQL Client will also print all the properties.
To distinguish the deprecated key, the sql client use the '[DEPRECATED]' as the identifier is deprecated.

```text
Flink SQL>SET;
execution.runtime-mode=batch
sql-client.execution.result-mode=table
table.planner=blink
[DEPRECATED] execution.planner=blink
[DEPRECATED] execution.result-mode=table
[DEPRECATED] execution.type=batch
```

{{< top >}}

Limitations & Future
--------------------

The current SQL Client only supports embedded mode. In the future, the community plans to extend its functionality by providing a REST-based SQL Client Gateway, see more in [FLIP-24](https://cwiki.apache.org/confluence/display/FLINK/FLIP-24+-+SQL+Client) and [FLIP-91](https://cwiki.apache.org/confluence/display/FLINK/FLIP-91%3A+Support+SQL+Client+Gateway).

{{< top >}}
