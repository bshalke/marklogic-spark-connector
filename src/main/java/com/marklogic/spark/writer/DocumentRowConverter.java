package com.marklogic.spark.writer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.spark.Util;
import com.marklogic.spark.reader.document.DocumentRowSchema;
import org.apache.spark.sql.catalyst.InternalRow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Knows how to build a document from a row corresponding to our {@code DocumentRowSchema}.
 */
class DocumentRowConverter implements RowConverter {

    private final ObjectMapper objectMapper;
    private final String uriTemplate;

    DocumentRowConverter(String uriTemplate) {
        this.uriTemplate = uriTemplate;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public Optional<DocBuilder.DocumentInputs> convertRow(InternalRow row) {
        final String uri = row.getString(0);
        if (row.isNullAt(1)) {
            Util.MAIN_LOGGER.warn("Not writing document with URI {} as it has null content; this will be supported " +
                "once the MarkLogic Java Client 6.6.1 is available.", uri);
            return Optional.empty();
        }
        final BytesHandle content = new BytesHandle(row.getBinary(1));
        String format = row.isNullAt(2) ? null : row.getString(2);
        // If the format isn't specified then a REST transform gets a binary node. That's not desirable. Think
        // we need to force a format for known URI extensions.
        // Perhaps have a "detect document type" option when using a transform?
        // And a way to specify document types - e.g.
        // spark.marklogic.write.transform.xmlExtensions=xml,xhtml,etc
        // spark.marklogic.write.transform.jsonExtensions=json
        // spark.marklogic.write.transform.textExtensions=txt,text
        if (Stream.of(".xml", ".html", ".xhtml", ".xsl", ".xslt").anyMatch(ext -> uri.endsWith(ext))) {
            content.withFormat(Format.XML);
        } else if (uri.endsWith(".json")) {
            content.withFormat(Format.JSON);
        }
        Optional<JsonNode> uriTemplateValues = deserializeContentToJson(uri, content, format);
        DocumentMetadataHandle metadata = DocumentRowSchema.makeDocumentMetadata(row);
        return Optional.of(new DocBuilder.DocumentInputs(uri, content, uriTemplateValues.orElse(null), metadata));
    }

    @Override
    public List<DocBuilder.DocumentInputs> getRemainingDocumentInputs() {
        return new ArrayList<>();
    }

    private Optional<JsonNode> deserializeContentToJson(String initialUri, BytesHandle contentHandle, String format) {
        if (this.uriTemplate == null || this.uriTemplate.trim().length() == 0 || contentHandle == null) {
            return Optional.empty();
        }
        try {
            JsonNode json = objectMapper.readTree(contentHandle.get());
            return Optional.of(json);
        } catch (IOException e) {
            // Preserves the initial support in the 2.2.0 release.
            ObjectNode values = objectMapper.createObjectNode();
            values.put("URI", initialUri);
            if (format != null) {
                values.put("format", format);
            }
            return Optional.of(values);
        }
    }
}
