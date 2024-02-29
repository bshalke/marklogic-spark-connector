package com.marklogic.spark.writer;

import com.codahale.metrics.Counter;
import com.marklogic.spark.Util;
import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.ExecutorPlugin;
import org.apache.spark.api.plugin.PluginContext;
import org.apache.spark.api.plugin.SparkPlugin;

import java.util.Map;

public class MyPlugin implements SparkPlugin {

    public static Counter counter;

    @Override
    public DriverPlugin driverPlugin() {
        return null;
    }

    @Override
    public ExecutorPlugin executorPlugin() {
        return new ExecutorPlugin() {
            @Override
            public void init(PluginContext pluginContext, Map<String, String> extraConf) {
                Util.MAIN_LOGGER.info("INIT PLUGIN!");
                counter = pluginContext.metricRegistry().counter("robSuccessCount");
                ExecutorPlugin.super.init(pluginContext, extraConf);
            }
        };
    }
}
