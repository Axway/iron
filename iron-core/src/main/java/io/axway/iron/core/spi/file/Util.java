package io.axway.iron.core.spi.file;

import java.io.*;

final class Util {

    static void rename(File from, File to) throws IOException {
        if (!from.renameTo(to)) {
            throw new IOException("Unable to rename " + from.getAbsolutePath() + " to " + to.getAbsolutePath());
        }
    }

    static File ensureDirectoryExists(File dir) {
        if (!dir.exists() && !dir.mkdirs() && !dir.exists()) {
            throw new UncheckedIOException(new IOException("Unable to create directory " + dir.getAbsolutePath()));
        }
        return dir;
    }

    private Util() {
    }
}
