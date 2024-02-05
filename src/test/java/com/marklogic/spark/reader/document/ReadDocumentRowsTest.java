package com.marklogic.spark.reader.document;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReadDocumentRowsTest extends AbstractIntegrationTest {

    @Test
    void performanceTest() {
        if (true) return;
        long start = System.currentTimeMillis();
        long count = startRead()
            .option(Options.CLIENT_URI, "admin:CHANGEME@CHANGEME:8015")
            .option(Options.READ_DOCUMENTS_COLLECTIONS, "address")
            .option(Options.READ_DOCUMENTS_PARTITION_STRATEGY, "pageRange")
            .option(Options.READ_NUM_PARTITIONS, "24")
            .option(Options.READ_BATCH_SIZE, "1250")
            .load()
            .count();

        logger.info("Time: " + (System.currentTimeMillis() - start));
        assertEquals(150000, count);
    }

    @Test
    void readByCollection() {
        Dataset<Row> rows = startRead()
            .option(Options.READ_DOCUMENTS_COLLECTIONS, "author")
            .load();

        assertEquals(15, rows.count());

        Row row = rows.filter("URI = '/author/author1.json'").collectAsList().get(0);
        assertEquals("/author/author1.json", row.getString(0));
        assertEquals("JSON", row.getString(2));
        JsonNode doc = readJsonContent(row);
        // Verify just a couple fields to ensure the JSON object is correct.
        assertEquals(4, doc.get("CitationID").asInt());
        assertEquals("Vivianne", doc.get("ForeName").asText());
    }

    @Test
    void invalidBatchSize() {
        // Verify batch size doesn't cause an error.
        assertEquals(15, startRead()
            .option(Options.READ_DOCUMENTS_COLLECTIONS, "author")
            .option(Options.READ_BATCH_SIZE, "10")
            .load()
            .count());

        // Then verify a nice error is thrown when the value is invalid.
        Dataset dataset = startRead()
            .option(Options.READ_DOCUMENTS_COLLECTIONS, "author")
            .option(Options.READ_BATCH_SIZE, "abc")
            .load();

        ConnectorException ex = assertThrowsConnectorException(() -> dataset.count());
        assertEquals("Invalid value for option spark.marklogic.read.batchSize: abc; must be numeric.", ex.getMessage());
    }

    @Test
    void noDocsInCollection() {
        long count = startRead()
            .option(Options.READ_DOCUMENTS_COLLECTIONS, "some-collection-with-no-documents")
            .load()
            .count();
        assertEquals(0, count);
    }

    @Test
    void stringQuery() {
        List<Row> rows = startRead()
            .option(Options.READ_DOCUMENTS_STRING_QUERY, "Vivianne OR Moria")
            .option(Options.READ_DOCUMENTS_PARTITION_STRATEGY, "pageRange")
            .option(Options.READ_NUM_PARTITIONS, "2")
            .option(Options.READ_BATCH_SIZE, "10")
            .load()
            .collectAsList();

        List<String> uris = getUrisFromRows(rows);
        assertEquals(2, uris.size());
        assertTrue(uris.contains("/author/author1.json"));
        assertTrue(uris.contains("/author/author11.json"));
    }

    @Test
    void structuredQueryXML() {
        String query = "<query xmlns='http://marklogic.com/appservices/search'>" +
            "<term-query><text>Vivianne</text></term-query></query>";
        List<Row> rows = startRead()
            .option(Options.READ_DOCUMENTS_QUERY, query)
            .option(Options.READ_DOCUMENTS_PARTITION_STRATEGY, "pageRange")
            .option(Options.READ_NUM_PARTITIONS, "2")
            .option(Options.READ_BATCH_SIZE, "10")
            .load()
            .collectAsList();

        assertEquals(1, rows.size());
        assertEquals("/author/author1.json", rows.get(0).getString(0));
    }

    @Test
    void structuredQueryWithStringQuery() {
        String query = "<query xmlns='http://marklogic.com/appservices/search'>" +
            "<term-query><text>Vivianne</text></term-query></query>";

        List<Row> rows = startRead()
            .option(Options.READ_DOCUMENTS_QUERY, query)
            .option(Options.READ_DOCUMENTS_STRING_QUERY, "Wooles")
            .option(Options.READ_DOCUMENTS_PARTITION_STRATEGY, "pageRange")
            .option(Options.READ_NUM_PARTITIONS, "2")
            .option(Options.READ_BATCH_SIZE, "10")
            .load()
            .collectAsList();

        assertEquals(1, rows.size());
        assertEquals("/author/author1.json", rows.get(0).getString(0));
    }

    @Test
    void structuredQueryWithCollection() {
        String query = "<query xmlns='http://marklogic.com/appservices/search'>" +
            "<term-query><text>Vivianne</text></term-query></query>";
        List<Row> rows = startRead()
            .option(Options.READ_DOCUMENTS_QUERY, query)
            .option(Options.READ_DOCUMENTS_COLLECTIONS, "author")
            .load()
            .collectAsList();

        assertEquals(1, rows.size());
    }

    @Test
    void structuredQueryWithNonMatchingCollection() {
        String query = "<query xmlns='http://marklogic.com/appservices/search'>" +
            "<term-query><text>Vivianne</text></term-query></query>";
        List<Row> rows = startRead()
            .option(Options.READ_DOCUMENTS_QUERY, query)
            .option(Options.READ_DOCUMENTS_COLLECTIONS, "some-other-collection")
            .load()
            .collectAsList();

        assertEquals(0, rows.size());
    }

    @Test
    void structuredQueryJSON() {
        String query = "{ \"query\": { \"queries\": [{ \"term-query\": { \"text\": [ \"Moria\" ] } }] } }";
        List<Row> rows = startRead()
            .option(Options.READ_DOCUMENTS_QUERY, query)
            .option(Options.READ_DOCUMENTS_PARTITION_STRATEGY, "pageRange")
            .option(Options.READ_DOCUMENTS_QUERY_FORMAT, "json")
            .option(Options.READ_NUM_PARTITIONS, "2")
            .option(Options.READ_BATCH_SIZE, "10")
            .load()
            .collectAsList();

        assertEquals(1, rows.size());
        assertEquals("/author/author11.json", rows.get(0).getString(0));
    }

    @Test
    void serializedCTSQueryXML() {
        String query = "<cts:word-query xmlns:cts='http://marklogic.com/cts'>" +
            "<cts:text xml:lang='en'>Vivianne</cts:text>" +
            "</cts:word-query>";

        List<Row> rows = startRead()
            .option(Options.READ_DOCUMENTS_QUERY, query)
            .option(Options.READ_DOCUMENTS_PARTITION_STRATEGY, "pageRange")
            .option(Options.READ_NUM_PARTITIONS, "2")
            .option(Options.READ_BATCH_SIZE, "10")
            .load()
            .collectAsList();

        assertEquals(1, rows.size());
    }

    @Test
    void serializedCTSQueryJSON() {
        String query = "{\"ctsquery\": {\"wordQuery\": {\"text\": \"Vivianne\"}}}";

        List<Row> rows = startRead()
            .option(Options.READ_DOCUMENTS_QUERY, query)
            .option(Options.READ_DOCUMENTS_PARTITION_STRATEGY, "pageRange")
            .option(Options.READ_DOCUMENTS_QUERY_FORMAT, "json")
            .option(Options.READ_NUM_PARTITIONS, "2")
            .option(Options.READ_BATCH_SIZE, "10")
            .load()
            .collectAsList();

        assertEquals(1, rows.size());
    }

    @Test
    void combinedQueryXML() {
        String query = "<search xmlns='http://marklogic.com/appservices/search'>" +
            "<options><constraint name='citation_id'>" +
            "<range type='xs:int'><json-property>CitationID</json-property></range>" +
            "</constraint></options>" +
            "<qtext>citation_id:3</qtext></search>";

        List<Row> rows = startRead()
            .option(Options.READ_DOCUMENTS_QUERY, query)
            .option(Options.READ_DOCUMENTS_PARTITION_STRATEGY, "pageRange")
            .option(Options.READ_NUM_PARTITIONS, "2")
            .option(Options.READ_BATCH_SIZE, "10")
            .load()
            .collectAsList();

        assertEquals(4, rows.size());
    }

    @Test
    void combinedQueryJSON() {
        ObjectNode combinedQuery = objectMapper.createObjectNode().putObject("search");
        ObjectNode constraint = combinedQuery.putObject("options").putObject("constraint");
        constraint.put("name", "citation_id");
        constraint.putObject("range").put("json-property", "CitationID");
        combinedQuery.put("qtext", "citation_id:3");

        List<Row> rows = startRead()
            .option(Options.READ_DOCUMENTS_QUERY, combinedQuery.toString())
            .option(Options.READ_DOCUMENTS_PARTITION_STRATEGY, "pageRange")
            .option(Options.READ_DOCUMENTS_QUERY_FORMAT, "json")
            .option(Options.READ_NUM_PARTITIONS, "2")
            .option(Options.READ_BATCH_SIZE, "10")
            .load()
            .collectAsList();

        assertEquals(4, rows.size());
    }

    @Test
    void directory() {
        List<Row> rows = startRead()
            .option(Options.READ_DOCUMENTS_DIRECTORY, "/author/")
            .load()
            .collectAsList();

        assertEquals(15, rows.size(), "Should retrieve all 15 documents in the '/author/' directory.");
    }

    @Test
    void directoryWithNoDocuments() {
        List<Row> rows = startRead()
            .option(Options.READ_DOCUMENTS_DIRECTORY, "/has-no-documents/")
            .load()
            .collectAsList();

        assertEquals(0, rows.size());
    }

    @Test
    void withSearchOptions() {
        List<Row> rows = startRead()
            .option(Options.READ_DOCUMENTS_STRING_QUERY, "citation_id:3")
            .option(Options.READ_DOCUMENTS_OPTIONS, "test-options")
            .load()
            .collectAsList();

        assertEquals(4, rows.size(), "4 authors have a CitationID of 3");
        List<String> uris = getUrisFromRows(rows);
        assertTrue(uris.contains("/author/author6.json"));
        assertTrue(uris.contains("/author/author10.json"));
        assertTrue(uris.contains("/author/author11.json"));
        assertTrue(uris.contains("/author/author15.json"));
    }

    /**
     * See BuildSearchQueryTest for additional tests for constructing a server transform.
     */
    @Test
    void withTransform() {
        List<Row> rows = startRead()
            .option(Options.READ_DOCUMENTS_STRING_QUERY, "Vivianne")
            .option(Options.READ_DOCUMENTS_TRANSFORM, "withParams")
            .option(Options.READ_DOCUMENTS_TRANSFORM_PARAMS, "param1;value,1;param2;value2")
            .option(Options.READ_DOCUMENTS_TRANSFORM_PARAMS_DELIMITER, ";")
            .load()
            .collectAsList();

        assertEquals(1, rows.size());
        JsonNode doc = readJsonContent(rows.get(0));

        String message = "Doc should have been transformed via withParams server transform: " + doc.toPrettyString();
        assertTrue(doc.has("content"), message);
        assertTrue(doc.has("params"), message);
        assertTrue(doc.has("context"), message);

        JsonNode params = doc.get("params");
        assertEquals("value,1", params.get("param1").asText());
        assertEquals("value2", params.get("param2").asText());
    }

    private DataFrameReader startRead() {
        return newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri());
    }

    private List<String> getUrisFromRows(List<Row> rows) {
        return rows.stream().map(row -> row.getString(0)).collect(Collectors.toList());
    }

    private JsonNode readJsonContent(Row row) {
        try {
            return objectMapper.readTree((byte[]) row.get(1));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
