package com.marklogic.spark.writer;

import com.marklogic.spark.Util;
import org.apache.spark.sql.connector.metric.CustomMetric;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;

import java.util.Arrays;

public class MyTaskMetric implements CustomTaskMetric, CustomMetric {

    private long value;

    public MyTaskMetric() {
        Util.MAIN_LOGGER.info("CREATED!");
    }

    public MyTaskMetric(long value) {
        this.value = value;
    }

    @Override
    public String description() {
        return "TODO";
    }

    @Override
    public String aggregateTaskMetrics(long[] taskMetrics) {
        return "Uhh: " + Arrays.asList(taskMetrics);
    }

    @Override
    public String name() {
        return "robSuccessCount";
    }

    @Override
    public long value() {
        return value;
    }
}
