package io.axway.iron.core.spi.file;

import java.nio.file.Paths;
import java.util.*;
import org.testng.annotations.Test;
import io.axway.iron.sample.Sample;
import io.axway.iron.spi.jackson.JacksonSerializer;

public class FileStoreTest {

    @Test
    public void shouldCreateCompanySequenceBeRight() throws Exception {
        String randomStoreName = "iron-store-" + UUID.randomUUID();
        FileStoreFactory fileStoreFactory = new FileStoreFactory(Paths.get("iron"), null);

        JacksonSerializer jacksonSerializer = new JacksonSerializer();
        Sample.checkThatCreateCompanySequenceIsRight(fileStoreFactory, jacksonSerializer, fileStoreFactory, jacksonSerializer, randomStoreName);
    }
}


