package io.axway.iron.spi.chronicle;

import java.io.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class Util {
    private static final Logger LOGGER = LoggerFactory.getLogger(Util.class);

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
