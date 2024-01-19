package com.marklogic.spark.writer.file;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.util.SerializableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;

/**
 * Writes each row, which is expected to represent a document, as a single file.
 */
class DocumentFileWriter implements DataWriter<InternalRow> {

    private static final Logger logger = LoggerFactory.getLogger(DocumentFileWriter.class);

    private final Map<String, String> properties;
    private final SerializableConfiguration hadoopConfiguration;

    DocumentFileWriter(Map<String, String> properties, SerializableConfiguration hadoopConfiguration) {
        this.properties = properties;
        this.hadoopConfiguration = hadoopConfiguration;
    }

    @Override
    public void write(InternalRow row) throws IOException {
        final Path path = makePath(row);
        if (logger.isTraceEnabled()) {
            logger.trace("Will write to: {}", path);
        }
        BufferedOutputStream outputStream = makeOutputStream(path);
        try {
            outputStream.write(row.getBinary(1));
        } finally {
            IOUtils.closeQuietly(outputStream);
        }
    }

    @Override
    public WriterCommitMessage commit() {
        return null;
    }

    @Override
    public void abort() {
        // No action to take.
    }

    @Override
    public void close() {
        // Nothing to close.
    }

    private Path makePath(InternalRow row) {
        String dir = properties.get("path");
        final String uri = row.getString(0);
        String path = FileUtil.makePathFromDocumentURI(uri);
        return path.charAt(0) == '/' ? new Path(dir + path) : new Path(dir, path);
    }

    private BufferedOutputStream makeOutputStream(Path path) throws IOException {
        FileSystem fileSystem = path.getFileSystem(this.hadoopConfiguration.value());
        // MLCP doesn't write .crc files, not sure yet if we want to default this to true or false.
        fileSystem.setWriteChecksum(false);
        // Copied from MLCP; testing shows an enormous improvement in performance when writing to local files by
        // using this approach.
        if (fileSystem instanceof LocalFileSystem) {
            File file = new File(path.toUri().getPath());
            if (!file.exists() && file.getParentFile() != null) {
                file.getParentFile().mkdirs();
            }
            return new BufferedOutputStream(new FileOutputStream(file, false));
        }
        return new BufferedOutputStream(fileSystem.create(path, false));
    }
}
