package com.marklogic.spark.reader.file;

import com.marklogic.spark.ConnectorException;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.util.SerializableConfiguration;

import java.io.IOException;
import java.util.Map;

class QuadsFileReader extends AbstractRdfFileReader implements PartitionReader<InternalRow> {

    private final QuadStreamReader quadStreamReader;

    QuadsFileReader(FilePartition partition, SerializableConfiguration hadoopConfiguration, Map<String, String> properties) {
        super(partition);
        final Path path = new Path(partition.getPath());
        try {
            this.inputStream = openStream(path, hadoopConfiguration, properties);
            this.quadStreamReader = new QuadStreamReader(partition.getPath(), inputStream);
        } catch (Exception e) {
            throw new ConnectorException(String.format("Unable to read RDF file at %s; cause: %s", path, e.getMessage()), e);
        }
    }

    @Override
    public boolean next() throws IOException {
        return quadStreamReader.hasNext();
    }

    @Override
    public InternalRow get() {
        return quadStreamReader.get();
    }
}