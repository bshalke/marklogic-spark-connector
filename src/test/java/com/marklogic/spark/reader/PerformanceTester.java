package com.marklogic.spark.reader;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Not a test, but a handy way to run ad hoc performance tests against the "employee" documents created by the default
 * configuration in the quick-table project. Feel free to adjust the parameters in this for any ML app and TDE view to
 * do some manual ad hoc testing.
 */
public class PerformanceTester {

    private final static Logger logger = LoggerFactory.getLogger(PerformanceTester.class);

    public static void main(String[] args) {
        final int sparkConcurrentTaskCount = 16;
        final String query = "op.fromView('demo','employee')";
//        final String query = "op.fromView('demo','employee').where(op.eq(op.col('job_description'), 'Technician'))";
//        final String query = "op.fromView('demo', 'employee').where(op.le(op.col('person_id'), 8))";
        final long partitionCount = 8;
        final long batchSize = 100000;

        final String host = args.length > 0 ? args[0] : "localhost";

        Dataset<Row> dataset = SparkSession.builder()
            .master(String.format("local[%d]", sparkConcurrentTaskCount))
            .getOrCreate()
            .read()
            .format("com.marklogic.spark")
            .option("spark.marklogic.client.host", host)
            .option("spark.marklogic.client.port", 8009)
            .option("spark.marklogic.client.username", "admin")
            .option("spark.marklogic.client.password", "admin")
            .option("spark.marklogic.client.authType", "digest")
            .option(ReadConstants.OPTIC_DSL, query)
            .option(ReadConstants.NUM_PARTITIONS, partitionCount)
            .option(ReadConstants.BATCH_SIZE, batchSize)
            .load();

        long now = System.currentTimeMillis();
        long count = dataset.count();
        long duration = System.currentTimeMillis() - now;
        logger.info("Duration: {}; row count: {}; rows per second: {}", duration, count,
            (double) count / ((double) duration / 1000));
//        rows.forEach(row -> logger.info(row.prettyJson()));
    }
}
