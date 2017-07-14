package io.axway.iron.spi.chronicle;

import java.io.*;

final class Util {

    static File ensureDirectoryExists(File dir) {
        if (!dir.exists() && !dir.mkdirs() && !dir.exists()) {
            throw new UncheckedIOException(new IOException("Unable to create directory " + dir.getAbsolutePath()));
        }
        return dir;
    }

    private Util() {
    }
}
