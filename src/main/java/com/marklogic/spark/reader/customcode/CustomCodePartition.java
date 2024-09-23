/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.customcode;

import org.apache.spark.sql.connector.read.InputPartition;

import java.io.Serializable;

public class CustomCodePartition implements InputPartition, Serializable {

    static final long serialVersionUID = 1;

    private String partition;

    public CustomCodePartition() {
    }

    public CustomCodePartition(String partition) {
        this.partition = partition;
    }

    public String getPartition() {
        return partition;
    }
}
