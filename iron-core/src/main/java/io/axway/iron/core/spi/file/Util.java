package io.axway.iron.core.spi.file;

import java.io.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class Util {
    private static final Logger LOGGER = LoggerFactory.getLogger(Util.class);

    static void rename(File from, File to) throws IOException {
        if (!from.renameTo(to)) {
            throw new IOException("Unable to rename " + from.getAbsolutePath() + " to " + to.getAbsolutePath());
        }
    }

    static File ensureDirectoryExists(File dir) {
        if (!dir.exists()) {
            LOGGER.info("Creating directory {path=\"{}\"}", dir.getAbsolutePath());
            if (!dir.mkdirs() && !dir.exists()) {
                throw new UncheckedIOException(new IOException("Unable to create directory " + dir.getAbsolutePath()));
            }
        }
        return dir;
    }

    private Util() {
    }
}
