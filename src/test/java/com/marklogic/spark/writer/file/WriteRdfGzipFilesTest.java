package com.marklogic.spark.writer.file;

import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WriteRdfGzipFilesTest extends AbstractIntegrationTest {

    @Test
    void gzip(@TempDir Path tempDir) {
        newSparkSession()
            .read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_TRIPLES_GRAPHS, "http://example.org/graph")
            .load()
            .repartition(1)
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_RDF_FILES_FORMAT, "nt")
            .option(Options.WRITE_FILES_COMPRESSION, "gzip")
            .mode(SaveMode.Append)
            .save(tempDir.toFile().getAbsolutePath());

        File[] files = tempDir.toFile().listFiles();
        assertEquals(1, files.length, "Expecting 1 gzip file due to repartition=1 producing one writer.");
        assertTrue(files[0].getName().endsWith(".nt.gz"), "Unexpected filename: " + files[0].getName());

        List<Row> rows = sparkSession.read().format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "rdf")
            .option(Options.READ_FILES_COMPRESSION, "gzip")
            .load(tempDir.toFile().getAbsolutePath())
            .collectAsList();

        System.out.println(rowsToString(rows));
        System.out.println(rows.size());
    }
}
