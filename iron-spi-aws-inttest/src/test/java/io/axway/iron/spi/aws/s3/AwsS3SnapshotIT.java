package io.axway.iron.spi.aws.s3;

import java.nio.file.Paths;
import org.testng.annotations.Test;
import io.axway.iron.core.spi.file.FileStoreFactory;
import io.axway.iron.sample.Sample;
import io.axway.iron.spi.aws.BaseInttest;
import io.axway.iron.spi.jackson.JacksonSerializer;

public class AwsS3SnapshotIT extends BaseInttest {

    @Test
    public void shouldCreateCompanySequenceSample() throws Exception {
        String storeName = initStoreName();
        FileStoreFactory ironFileStoreFactory = new FileStoreFactory(Paths.get("iron"));
        JacksonSerializer jacksonSerializer = new JacksonSerializer();

        Sample.checkThatCreateCompanySequenceIsRight(ironFileStoreFactory, jacksonSerializer, ironFileStoreFactory, jacksonSerializer, storeName);
    }

    @Test
    public void shouldCreateCompanySequenceBeRightWithS3() throws Exception {
        String storeName = initStoreName();
        createS3Bucket(storeName);
        AwsS3SnapshotStoreFactory s3SnapshotStoreFactory = new AwsS3SnapshotStoreFactory(m_configuration);
        FileStoreFactory ironFileStoreFactory = new FileStoreFactory(Paths.get("iron"));
        JacksonSerializer jacksonSerializer = new JacksonSerializer();

        try {
            Sample.checkThatCreateCompanySequenceIsRight(ironFileStoreFactory, jacksonSerializer, s3SnapshotStoreFactory, jacksonSerializer, storeName);
        } finally {
            deleteS3Bucket(storeName);
        }
    }

    @Test
    public void shouldListSnapshotsReturnTheRightNumberOfSnapshotsSample() throws Exception {
        String storeName = initStoreName();
        FileStoreFactory ironFileStoreFactory = new FileStoreFactory(Paths.get("iron"));
        JacksonSerializer jacksonSerializer = new JacksonSerializer();

        Sample.checkThatListSnapshotsReturnTheRightNumberOfSnapshots(ironFileStoreFactory, jacksonSerializer, ironFileStoreFactory, jacksonSerializer,
                                                                     storeName);
    }

    @Test
    public void shouldListSnapshotsReturnTheRightNumberOfSnapshotsWithS3() throws Exception {
        String storeName = initStoreName();
        createS3Bucket(storeName);
        AwsS3SnapshotStoreFactory s3SnapshotStoreFactory = new AwsS3SnapshotStoreFactory(m_configuration);
        FileStoreFactory ironFileStoreFactory = new FileStoreFactory(Paths.get("iron"));
        JacksonSerializer jacksonSerializer = new JacksonSerializer();

        try {
            Sample.checkThatListSnapshotsReturnTheRightNumberOfSnapshots(ironFileStoreFactory, jacksonSerializer, s3SnapshotStoreFactory, jacksonSerializer,
                                                                         storeName);
        } finally {
            deleteS3Bucket(storeName);
        }
    }

    private String initStoreName() {
        String storeName = createRandomStoreName();
        m_configuration.setProperty(AwsS3Properties.S3_BUCKET_NAME_KEY.getPropertyKey(), storeName);
        return storeName;
    }
}
