package io.axway.iron.spi.file;

import java.nio.file.Paths;
import java.util.*;
import org.testng.annotations.Test;
import io.axway.iron.core.spi.file.FileStoreFactory;
import io.axway.iron.spi.SpiTest;
import io.axway.iron.spi.jackson.JacksonSerializer;

public class FileStoreTest {

    @Test
    public void shouldCreateCompanySequenceBeRight() throws Exception {
        String randomStoreName = "iron-store-" + UUID.randomUUID();
        FileStoreFactory fileStoreFactory = new FileStoreFactory(Paths.get("iron", "iron-spi-file-inttest"), null);

        JacksonSerializer jacksonSerializer = new JacksonSerializer();
        SpiTest.checkThatCreateCompanySequenceIsRight(fileStoreFactory, jacksonSerializer, fileStoreFactory, jacksonSerializer, randomStoreName);
    }
}


