/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import org.apache.spark.sql.connector.write.WriterCommitMessage;

import java.util.Set;

public class CommitMessage implements WriterCommitMessage {

    private final int successItemCount;
    private final int failedItemCount;
    private final Set<String> graphs;

    /**
     * @param successItemCount
     * @param failedItemCount
     * @param graphs           zero or more MarkLogic Semantics graph names, each of which is associated with a
     *                         graph document in MarkLogic that must be created after all the documents have been
     *                         written.
     */
    public CommitMessage(int successItemCount, int failedItemCount, Set<String> graphs) {
        this.successItemCount = successItemCount;
        this.failedItemCount = failedItemCount;
        this.graphs = graphs;
    }

    int getSuccessItemCount() {
        return successItemCount;
    }

    int getFailedItemCount() {
        return failedItemCount;
    }

    Set<String> getGraphs() {
        return graphs;
    }
}
