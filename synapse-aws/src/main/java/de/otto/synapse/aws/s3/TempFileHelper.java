package de.otto.synapse.aws.s3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

import static com.google.common.base.StandardSystemProperty.JAVA_IO_TMPDIR;

public class TempFileHelper {
    private static final Logger LOG = LoggerFactory.getLogger(TempFileHelper.class);

    public Path getTempFile(String filename) {
        return Paths.get(getTempDir() + "/" + filename);
    }

    String getTempDir() {
        return System.getProperty(JAVA_IO_TMPDIR.key());
    }

    public boolean existsAndHasSize(Path path, long size) {
        File file = path.toFile();
        return file.exists() && file.canRead() && file.length() == size;
    }

    public void removeTempFiles(String filePattern) {
        String tmpDir = System.getProperty("java.io.tmpdir");
        final PathMatcher matcher = FileSystems.getDefault().getPathMatcher(String.format("glob:%s/%s", tmpDir, filePattern));
        try {
            Files.walkFileTree(Paths.get(tmpDir), new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    try {
                        if (matcher.matches(file)) {
                            LOG.info("delete file: " + file);
                            Files.delete(file);
                        }
                    } catch (IOException e) {
                        LOG.warn("could not delete file: " + file, e);
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            LOG.error("error deleting tempfiles", e);
        }
    }
}
