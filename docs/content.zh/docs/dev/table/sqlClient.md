---
title: "SQL 客户端"
weight: 91
type: docs
aliases:
  - /zh/dev/table/sqlClient.html
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

# SQL 客户端

Flink 的 Table & SQL API 可以处理 SQL 语言编写的查询语句，但是这些查询需要嵌入用 Java 或 Scala 编写的表程序中。此外，这些程序在提交到集群前需要用构建工具打包。这或多或少限制了 Java/Scala 程序员对 Flink 的使用。

*SQL 客户端* 的目的是提供一种简单的方式来编写、调试和提交表程序到 Flink 集群上，而无需写一行 Java 或 Scala 代码。*SQL 客户端命令行界面（CLI）* 能够在命令行中检索和可视化分布式应用中实时产生的结果。

<a href="/fig/sql_client_demo.gif"><img class="offset" src="/fig/sql_client_demo.gif" alt="Animated demo of the Flink SQL Client CLI running table programs on a cluster" width="80%" /></a>



入门
---------------

本节介绍如何在命令行里启动（setup）和运行你的第一个 Flink SQL 程序。

SQL 客户端捆绑在常规 Flink 发行版中，因此可以直接运行。它仅需要一个正在运行的 Flink 集群就可以在其中执行表程序。有关设置 Flink 群集的更多信息，请参见[集群和部署]({{< ref "docs/deployment/resource-providers/standalone/overview" >}})部分。如果仅想试用 SQL 客户端，也可以使用以下命令启动本地集群：

```bash
./bin/start-cluster.sh
```

### 启动 SQL 客户端命令行界面

SQL Client 脚本也位于 Flink 的 bin 目录中。[将来](sqlClient.html#limitations--future)，用户可以通过启动嵌入式 standalone 进程或通过连接到远程 SQL 客户端网关来启动 SQL 客户端命令行界面。目前仅支持 `embedded`，模式默认值`embedded`。可以通过以下方式启动 CLI：

```bash
./bin/sql-client.sh
```

或者显式使用 `embedded` 模式:

```bash
./bin/sql-client.sh embedded
```

默认情况下，SQL 客户端将从 `./conf/sql-client-defaults.yaml` 中读取配置。有关环境配置文件结构的更多信息，请参见[配置部分](sqlClient.html#environment-files)。

### 执行 SQL 查询

命令行界面启动后，你可以使用 `HELP` 命令列出所有可用的 SQL 语句。输入第一条 SQL 查询语句并按 `Enter` 键执行，可以验证你的设置及集群连接是否正确：

```sql
SELECT 'Hello World';
```

该查询不需要 table source，并且只产生一行结果。CLI 将从集群中检索结果并将其可视化。按 `Q` 键退出结果视图。

CLI 为维护和可视化结果提供**三种模式**。

**表格模式**（table mode）在内存中实体化结果，并将结果用规则的分页表格可视化展示出来。执行如下命令启用：

```text
SET execution.result-mode=table;
```

**变更日志模式**（changelog mode）不会实体化和可视化结果，而是由插入（`+`）和撤销（`-`）组成的持续查询产生结果流。

```text
SET execution.result-mode=changelog;
```

**Tableau模式**（tableau mode）更接近传统的数据库，会将执行的结果以制表的形式直接打在屏幕之上。具体显示的内容会取决于作业
执行模式的不同(`execution.type`)：

```text
SET execution.result-mode=tableau;
```

注意当你使用这个模式运行一个流式查询的时候，Flink 会将结果持续的打印在当前的屏幕之上。如果这个流式查询的输入是有限的数据集，
那么Flink在处理完所有的数据之后，会自动的停止作业，同时屏幕上的打印也会相应的停止。如果你想提前结束这个查询，那么可以直接使用
`CTRL-C` 按键，这个会停掉作业同时停止屏幕上的打印。

你可以用如下查询来查看三种结果模式的运行情况：

```sql
SELECT name, COUNT(*) AS cnt FROM (VALUES ('Bob'), ('Alice'), ('Greg'), ('Bob')) AS NameTable(name) GROUP BY name;
```

此查询执行一个有限字数示例：

*变更日志模式* 下，看到的结果应该类似于：

```text
+ Bob, 1
+ Alice, 1
+ Greg, 1
- Bob, 1
+ Bob, 2
```

*表格模式* 下，可视化结果表将不断更新，直到表程序以如下内容结束：

```text
Bob, 2
Alice, 1
Greg, 1
```

*Tableau模式* 下，如果这个查询以流的方式执行，那么将显示以下内容：
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

如果这个查询以批的方式执行，显示的内容如下：
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

这几种结果模式在 SQL 查询的原型设计过程中都非常有用。这些模式的结果都存储在 SQL 客户端 的 Java 堆内存中。为了保持 CLI 界面及时响应，变更日志模式仅显示最近的 1000 个更改。表格模式支持浏览更大的结果，这些结果仅受可用主内存和配置的[最大行数](sqlClient.html#configuration)（`max-table-result-rows`）的限制。

<span class="label label-danger">注意</span> 在批处理环境下执行的查询只能用表格模式或者Tableau模式进行检索。

定义查询语句后，可以将其作为长时间运行的独立 Flink 作业提交给集群。为此，其目标系统需要使用 [INSERT INTO 语句](sqlClient.html#detached-sql-queries)指定存储结果。[配置部分](sqlClient.html#configuration)解释如何声明读取数据的 table source，写入数据的 sink 以及配置其他表程序属性的方法。

{{< top >}}

<a name="configuration"></a>

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
- defines an environment with a table source `MyTableSource` that reads from a CSV file,
- defines a view `MyCustomView` that declares a virtual table using a SQL query,
- defines a user-defined function `myUDF` that can be instantiated using the class name and two constructor parameters,
- uses the blink planner in streaming mode for running statements and a parallelism of 1,
- runs exploratory queries in the `table` result mode,
- and makes some planner adjustments around join reordering and spilling via configuration options.

When using -i option to submit SQL file, the type of the statement is limited. It only allow users to execute statement:
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

SQL Client allows users to submit jobs either within the CLI session or using -f option to submit sql file.

In both modes, SQL Client supports to parse and execute all types of the Flink supported SQL statements.

### Interactive Mode

In interactive mode, the SQL Client reads user inputs and executes the statement when meets the statement splitter.

When execute the statement succeeds, the SQL Client will print the message to inform. Otherwise, the SQL Client will
print the causes why the execution fails. For detailed exception stack, please set the `sql-client.verbose` true.

### Execute SQL Files

When start up the SQL Client with -f option, the SQL Client will split the content of the specified file into multiple statements and submit them line by line.
The SQL Client will print all the messages as the message printed in the interactive mode.
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
INSERT INTO MyTableSink SELECT * FROM MyTableSource;
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

To be compatible with before, SQL Client still supports to import the YAML file and allows to SET the key in YAML file.
When set the key defined in YAML file, the SQL Client will print the warning messages to inform.

```text
Flink SQL> SET execution.type = batch;
[WARNING] The specified key 'execution.type' is deprecated. Please use 'execution.runtime-mode' instead.
[INFO] Session property has been set.
```

When using `SET` command to print the properties, the SQL Client will also print all the properties.
To distinguish the deprecated key, the sql client use the '[DEPRECATED]' as the identifier.deprecated

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

<a name="limitations--future"></a>

局限与未来
--------------------

当前的 SQL 客户端仅支持嵌入式模式。在将来，社区计划提供基于 REST 的 [SQL 客户端网关（Gateway）](sqlClient.html#limitations--future) 的功能，详见 [FLIP-24](https://cwiki.apache.org/confluence/display/FLINK/FLIP-24+-+SQL+Client) 和 [FLIP-91](https://cwiki.apache.org/confluence/display/FLINK/FLIP-91%3A+Support+SQL+Client+Gateway)。

{{< top >}}
