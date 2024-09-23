/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.optic;

import com.marklogic.spark.Options;
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
class PerformanceTester {

    private static final Logger logger = LoggerFactory.getLogger(PerformanceTester.class);

    public static void main(String[] args) {
        final int sparkConcurrentTaskCount = 16;
        final String query = "op.fromView('demo','employee')";
        final long partitionCount = 8;
        final long batchSize = 100000;

        final String host = args.length > 0 ? args[0] : "localhost";

        Dataset<Row> dataset = SparkSession.builder()
            .master(String.format("local[%d]", sparkConcurrentTaskCount))
            .getOrCreate()
            .read()
            .format("marklogic")
            .option(Options.CLIENT_HOST, host)
            .option(Options.CLIENT_PORT, 8009)
            .option(Options.CLIENT_USERNAME, "admin")
            .option(Options.CLIENT_PASSWORD, "admin")
            .option(Options.READ_OPTIC_QUERY, query)
            .option(Options.READ_NUM_PARTITIONS, partitionCount)
            .option(Options.READ_BATCH_SIZE, batchSize)
            .load();

        long now = System.currentTimeMillis();
        long count = dataset.count();
        long duration = System.currentTimeMillis() - now;
        logger.info("Duration: {}; row count: {}; rows per second: {}", duration, count,
            (double) count / ((double) duration / 1000));
    }
}
