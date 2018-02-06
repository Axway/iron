package io.axway.iron.core.spi.file;

import java.util.regex.*;
import javax.annotation.*;

import static java.util.regex.Pattern.*;

class FilenameUtils {

    static Pattern buildFilenamePattern(@Nullable Integer transactionIdLength) {
        String cardinality = transactionIdLength == null ? "+" : ("{" + transactionIdLength + "}");
        return compile("([0-9]" + cardinality + ").([a-z]+)");
    }

    static String buildFilenameFormat(@Nullable Integer transactionIdLength) {
        return transactionIdLength == null ? "%d.%s" : ("%0" + transactionIdLength + "d.%s");
    }
}
