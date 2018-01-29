package io.axway.iron.core.spi.file;

import java.util.regex.*;
import javax.annotation.*;

class FilenameUtils {

    static Pattern buildFilenamePattern(@Nullable Integer limitedSize) {
        String cardinality = limitedSize == null ? "+" : ("{" + limitedSize + "}");
        return Pattern.compile("([0-9]" + cardinality + ").([a-z]+)");
    }

    static String buildFilenameFormat(@Nullable Integer limitedSize) {
        return limitedSize == null ? "%d.%s" : ("%0" + limitedSize + "d.%s");
    }
}
