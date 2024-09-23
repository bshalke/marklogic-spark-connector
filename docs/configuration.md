---
layout: default
title: Configuration Reference
nav_order: 5
---

The MarkLogic connector has 3 sets of configuration options - connection options, reading options, and writing 
options. Each set of options is defined below.

## Table of contents
{: .no_toc .text-delta }

- TOC
{:toc}

## Referencing the connector

Starting with the 2.2.0 release, you can reference the MarkLogic connector via the short name "marklogic":

    session.read.format("marklogic")

Prior to 2.2.0, you must use the full name:

    session.read.format("com.marklogic.spark")

## Connection options

These options define how the connector connects and authenticates with MarkLogic.

| Option | Description | 
| --- | --- |
| spark.marklogic.client.host                 | Required; the host name to connect to; this can be the name of a host in your MarkLogic cluster or the host name of a load balancer. |
| spark.marklogic.client.port                 | Required; the port of the app server in MarkLogic to connect to. |
| spark.marklogic.client.basePath             | Base path to prefix on each request to MarkLogic. |
| spark.marklogic.client.database             | Name of the database to interact with; only needs to be set if it differs from the content database assigned to the app server that the connector will connect to.|
| spark.marklogic.client.connectionType       | Either `gateway` for when connecting to a load balancer, or `direct` when connecting directly to MarkLogic. Defaults to `gateway` which works in either scenario. |
| spark.marklogic.client.authType             | Required; one of `basic`, `digest`, `cloud`, `kerberos`, `certificate`, or `saml`. Defaults to `digest`. |
| spark.marklogic.client.username             | Required for `basic` and `digest` authentication. |
| spark.marklogic.client.password             | Required for `basic` and `digest` authentication. |
| spark.marklogic.client.certificate.file     | Required for `certificate` authentication; the path to a certificate file. |
| spark.marklogic.client.certificate.password | Required for `certificate` authentication; the password for accessing the certificate file. |
| spark.marklogic.client.cloud.apiKey         | Required for MarkLogic `cloud` authentication. |
| spark.marklogic.client.kerberos.principal | Required for `kerberos` authentication. |
| spark.marklogic.client.saml.token | Required for `saml` authentication. |
| spark.marklogic.client.sslEnabled | If 'true', an SSL connection is created using the JVM's default SSL context.
| spark.marklogic.client.sslHostnameVerifier | Either `any`, `common`, or `strict`; see the [MarkLogic Java Client documentation](https://docs.marklogic.com/javadoc/client/com/marklogic/client/DatabaseClientFactory.SSLHostnameVerifier.html) for more information on these choices. |
| spark.marklogic.client.ssl.keystore.path | File path to a Java keystore for 2-way SSL; since 2.1.0. |
| spark.marklogic.client.ssl.keystore.password | Optional password for a Java keystore for 2-way SSL; since 2.1.0. |
| spark.marklogic.client.ssl.keystore.type | Java keystore type for 2-way SSL; defaults to "JKS"; since 2.1.0. |
| spark.marklogic.client.ssl.keystore.algorithm | Java keystore algorithm for 2-way SSL; defaults to "SunX509"; since 2.1.0. |
| spark.marklogic.client.uri | Shortcut for setting the host, port, username, and password when using `basic` or `digest` authentication. See below for more information. |

### Connecting with a client URI

The `spark.marklogic.client.uri` is a convenience for the common case of using `basic` or `digest` authentication.
It allows you to specify username, password, host, and port via the following syntax:

    username:password@host:port

This avoids the need to set the individual options for the above 4 properties.  

You may also configure a database name via the following syntax:

    username:password@host:port/database

A database name is only needed when you wish to work with a database other than the one associated with the app server 
that the connector will connect to via the `port` value. 

Using this convenience can provide a much more succinct set of options - for example:

```
df = spark.read.format("marklogic")\
    .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8003")\
    .option("spark.marklogic.read.opticQuery", "op.fromView('example', 'employee')")\
    .load()
```

Note that if the username or password contain either a `@` or a `:` character, you must first convert them using 
[percent encoding](https://developer.mozilla.org/en-US/docs/Glossary/percent-encoding) into the correct character 
triplet. For example, a password of `sp@r:k` must appear in the `spark.marklogic.client.uri` string as `sp%40r%3Ak`. 

### Configuring SSL 

If the MarkLogic app server that the connector will connect to requires SSL but does not require that the client
present a certificate, set the `spark.marklogic.client.sslEnabled` option to 'true'. This causes the associated JVM's certificate store - typically the `$JAVA_HOME/jre/lib/security/cacerts` file - to be used for establishing an SSL connection. The certificate store should contain the public certificate associated with the SSL certificate template 
used by the MarkLogic app server. 

Starting in 2.1.0, if the MarkLogic app server requires the client to present a certificate, set the 
`spark.marklogic.client.ssl.keystore.path` option to point to a Java keystore containing the client certificate. 
Set `spark.marklogic.client.ssl.keystore.password` if the keystore requires a password. The keystore will also be used
as the truststore so it must also contain the public certificate associated with the SSL certificate template used
by the MarkLogic app server. A future release of the connector will allow for the truststore to be a separate file.

If you receive an error containing a message of "PKIX path building failed", the most likely issue is that your JVM's
certificate store does not contain the public certificate associated with the MarkLogic app server, or your Spark
environment may be using a JVM different from the one you think it is. 
[This guide](https://www.baeldung.com/jvm-certificate-store-errors) provides some common solutions for solving this
error. 

If you receive an `javax.net.ssl.SSLPeerUnverifiedException` error, you will need to adjust the 
`spark.marklogic.client.sslHostnameVerifier` option. A value of `ANY` will disable hostname verification, 
which may be appropriate in a development or test environment. The 
[MarkLogic Java Client documentation](https://docs.marklogic.com/javadoc/client/com/marklogic/client/DatabaseClientFactory.SSLHostnameVerifier.html)
describes the other choices for this option.

## Read options

See [the guide on reading](reading-data/reading.md) for more information on how data is read from MarkLogic.

### Read options for Optic queries

The following options control how the connector reads rows from MarkLogic via an Optic query:

| Option | Description | 
| --- | --- |
| spark.marklogic.read.batchSize | Approximate number of rows to retrieve in each call to MarkLogic; defaults to 100000. |
| spark.marklogic.read.numPartitions | The number of Spark partitions to create; defaults to `spark.default.parallelism`. |
| spark.marklogic.read.opticQuery | Required; the Optic DSL query to run for retrieving rows; must use `op.fromView` as the accessor. |
| spark.marklogic.read.pushDownAggregates | Whether to push down aggregate operations to MarkLogic; defaults to `true`. Set to `false` to prevent aggregates from being pushed down to MarkLogic. |

### Read options for custom code

The following options control how the connector reads rows from MarkLogic via custom code:

| Option | Description | 
| --- | --- |
| spark.marklogic.read.invoke | The path to a module to invoke; the module must be in your application's modules database. |
| spark.marklogic.read.javascript | JavaScript code to execute. |
| spark.marklogic.read.javascriptFile | Local file path containing JavaScript code to execute. |
| spark.marklogic.read.xquery | XQuery code to execute. |
| spark.marklogic.read.xqueryFile | Local file path containing XQuery code to execute. |
| spark.marklogic.read.vars. | Prefix for user-defined variables to be sent to the custom code. |

If you are using Spark's streaming support with custom code, or you need to break up your custom code query into 
multiple queries, the following options can also be used to control how partitions are defined:

| Option | Description | 
| --- | --- |
| spark.marklogic.read.partitions.invoke | The path to a module to invoke; the module must be in your application's modules database. |
| spark.marklogic.read.partitions.javascript | JavaScript code to execute. |
| spark.marklogic.read.partitions.javascriptFile | Local file path containing JavaScript code to execute. |
| spark.marklogic.read.partitions.xquery | XQuery code to execute. |
| spark.marklogic.read.partitions.xqueryFile | Local file path containing XQuery code to execute. |

### Read options for documents

The following options control how the connector reads document rows from MarkLogic via search queries:

| Option | Description | 
| --- | --- |
| spark.marklogic.read.documents.stringQuery | A [MarkLogic string query](https://docs.marklogic.com/guide/search-dev/string-query) for selecting documents. |
| spark.marklogic.read.documents.query | A JSON or XML representation of a [structured query](https://docs.marklogic.com/guide/search-dev/structured-query#), [serialized CTS query](https://docs.marklogic.com/guide/rest-dev/search#id_30577), or [combined query](https://docs.marklogic.com/guide/rest-dev/search#id_69918). |
| spark.marklogic.read.documents.categories | Controls which metadata is returned for each document. Defaults to `content`. Allowable values are `content`, `metadata`, `collections`, `permissions`, `quality`, `properties`, and `metadatavalues`. |
| spark.marklogic.read.documents.collections | Comma-delimited string of zero to many collections to constrain the query. |
| spark.marklogic.read.documents.directory | Database directory - e.g. "/company/employees/" - to constrain the query. |
| spark.marklogic.read.documents.filtered | Set to true for [filtered searches](https://docs.marklogic.com/guide/performance/unfiltered). Defaults to `false` as unfiltered searches are significantly faster and will produce accurate results when your application indexes are sufficient for your query. |
| spark.marklogic.read.documents.options | Name of a set of [MarkLogic search options](https://docs.marklogic.com/guide/search-dev/query-options) to be applied against a string query. |
| spark.marklogic.read.documents.partitionsPerForest | Number of Spark partition readers to create per forest; defaults to 4. |
| spark.marklogic.read.documents.transform | Name of a [MarkLogic REST transform](https://docs.marklogic.com/guide/rest-dev/transforms) to apply to each matching document. |
| spark.marklogic.read.documents.transformParams | Comma-delimited sequence of transform parameter names and values - e.g. `param1,value1,param2,value`. |
| spark.marklogic.read.documents.transformParamsDelimiter | Delimiter for transform parameters; defaults to a comma. |

### Read options for files

As of the 2.3.0 release, the connector supports reading aggregate XML files, RDF files, and ZIP files. The following
options control how the connector reads files:

| Option | Description | 
| --- | --- |
| spark.marklogic.read.aggregates.xml.element | Required when reading aggregate XML files; defines the name of the element for selecting elements to convert into Spark rows. |
| spark.marklogic.read.aggregates.xml.namespace | Optional namespace for the element identified by `spark.marklogic.read.aggregates.xml.element`. |
| spark.marklogic.read.aggregates.xml.uriElement | Optional element name for constructing a URI based on an element value. |
| spark.marklogic.read.aggregates.xml.uriNamespace | Optional namespace for the element identified by `spark.marklogic.read.aggregates.xml.uriElement`. |
| spark.marklogic.read.files.abortOnFailure | Set to `false` so that the connector logs errors and continues processing files. Defaults to `true`. |
| spark.marklogic.read.files.compression | Set to `gzip` or `zip` when reading compressed files. |
| spark.marklogic.read.files.type | Set to `rdf` when reading RDF files. This option only needs to be set when the connector is otherwise unable to detect that it should perform some sort of handling for the file. |


## Write options

See [the guide on writing](writing.md) for more information on how data is written to MarkLogic.

### Writing rows as documents to MarkLogic

The following options control how the connector writes rows as documents to MarkLogic:

| Option | Description | 
| --- | --- |
| spark.marklogic.write.abortOnFailure | Whether the Spark job should abort if a batch fails to be written; defaults to `true`. |
| spark.marklogic.write.batchSize | The number of documents written in a call to MarkLogic; defaults to 100. |
| spark.marklogic.write.collections | Comma-delimited string of collection names to add to each document. |
| spark.marklogic.write.permissions | Comma-delimited string of role names and capabilities to add to each document - e.g. role1,read,role2,update,role3,execute . |
| spark.marklogic.write.fileRows.documentType | Forces a document type when MarkLogic does not recognize a URI extension; must be one of `JSON`, `XML`, or `TEXT`. |
| spark.marklogic.write.jsonRootName | As of 2.3.0, specifies a root field name when writing JSON documents based on arbitrary rows. |
| spark.marklogic.write.temporalCollection | Name of a temporal collection to assign each document to. |
| spark.marklogic.write.threadCount | The number of threads used across all partitions to send documents to MarkLogic; defaults to 4. |
| spark.marklogic.write.threadCountPerPartition | New in 2.3.0; the number of threads used per partition to send documents to MarkLogic. |
| spark.marklogic.write.transform | Name of a REST transform to apply to each document. |
| spark.marklogic.write.transformParams | Comma-delimited string of transform parameter names and values - e.g. param1,value1,param2,value2 . |
| spark.marklogic.write.transformParamsDelimiter | Delimiter to use instead of a command for the `transformParams` option. |
| spark.marklogic.write.uriPrefix | String to prepend to each document URI, where the URI defaults to a UUID. |
| spark.marklogic.write.uriReplace | Modify the initial URI for a row via a comma-delimited list of regular expression and replacement string pairs - e.g. regex,'value',regex,'value'. Each replacement string must be enclosed by single quotes. |
| spark.marklogic.write.uriSuffix | String to append to each document URI, where the URI defaults to a UUID. |
| spark.marklogic.write.uriTemplate | String defining a template for constructing each document URI. See [Writing data](writing.md) for more information. |

### Processing rows via custom code

The following options control how rows can be processed with custom code in MarkLogic:

| Option | Description | 
| --- | --- |
| spark.marklogic.write.abortOnFailure | Whether the Spark job should abort if a batch fails to be written; defaults to `true`. |
| spark.marklogic.write.batchSize | The number of rows sent in a call to MarkLogic; defaults to 1. |
| spark.marklogic.write.invoke | The path to a module to invoke; the module must be in your application's modules database. |
| spark.marklogic.write.javascript | JavaScript code to execute. |
| spark.marklogic.write.javascriptFile | Local file path containing JavaScript code to execute. |
| spark.marklogic.write.xquery | XQuery code to execute. |
| spark.marklogic.write.xqueryFile | Local file path containing XQuery code to execute. |
| spark.marklogic.write.externalVariableName | Name of the external variable in custom code that is populated with row values; defaults to `URI`. |
| spark.marklogic.write.externalVariableDelimiter | Delimiter used when multiple row values are sent in a single call; defaults to a comma. |
| spark.marklogic.write.vars. | Prefix for user-defined variables to be sent to the custom code. |
