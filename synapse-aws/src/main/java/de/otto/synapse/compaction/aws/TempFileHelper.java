package de.otto.synapse.compaction.aws;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.stream.Collectors;

import static com.google.common.base.StandardSystemProperty.JAVA_IO_TMPDIR;
import static java.lang.String.format;
import static java.nio.file.Files.delete;

public final class TempFileHelper {
    private static final Logger LOG = LoggerFactory.getLogger(TempFileHelper.class);
    private static final int ONE_MB = 1024 * 1024;

    private TempFileHelper() {
    }

    public static Path getTempFile(String filename) {
        return Paths.get(getTempDir() + "/" + filename);
    }

    public static String getTempDir() {
        return System.getProperty(JAVA_IO_TMPDIR.key());
    }

    public static boolean existsAndHasSize(Path path, long size) {
        File file = path.toFile();
        return file.exists() && file.canRead() && file.length() == size;
    }

    public static void removeTempFiles(String filePattern) {
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

    public static void logDiskUsage() {
        File file = null;
        try {
            file = File.createTempFile("tempFileForDiskUsage", ".txt");
            float usableSpace = (float) file.getUsableSpace() / 1024 / 1024 / 1024;
            float freeSpace = (float) file.getFreeSpace() / 1024 / 1024 / 1024;
            LOG.info(format("Available DiskSpace: usable %.3f GB / free %.3f GB", usableSpace, freeSpace));

            String tempDirContent = Files.list(Paths.get(System.getProperty("java.io.tmpdir")))
                    .filter(path -> path.toFile().isFile())
                    .filter(path -> path.toFile().length() > ONE_MB)
                    .map(path -> String.format("%s %dmb", path.toString(), path.toFile().length() / ONE_MB))
                    .collect(Collectors.joining("\n"));
            LOG.info("files in /tmp > 1mb: \n {}", tempDirContent);

        } catch (IOException e) {
            LOG.info("Error calculating disk usage: " + e.getMessage());
        } finally {
            try {
                if (file != null) {
                    LOG.info("delete file {}", file.toPath().toString());
                    delete(file.toPath());
                }
            } catch (IOException e) {
                LOG.error("Error deleting temp file while calculating disk usage:" + e.getMessage());
            }
        }
    }

}
