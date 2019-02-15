package io.axway.iron.spi.aws.s3;

import java.math.BigInteger;
import java.util.*;
import org.testng.annotations.Test;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import mockit.Expectations;
import mockit.Mocked;

import static org.assertj.core.api.Assertions.assertThat;

public class AwsS3SnapshotStoreTest {

    @Mocked
    public AmazonS3 m_amazonS3;

    @Mocked
    public AwsS3SnapshotStore.StoreLocker m_storeLocker;

    @Test
    public void shouldListNotLockedSnapshotStores() {
        new Expectations() {{
            m_amazonS3.listObjectsV2((ListObjectsV2Request) any).getCommonPrefixes();
            result = List.of("blabla/snapshot/123456789012345678901234567890/",//
                             "blabla/snapshot/123456789012345678901234567891/",//
                             "blabla/snapshot/123456789012345678901234567892/");
            m_storeLocker.isStoreLocked((BigInteger) any);
            result = false;// first store not locked
            result = true;// second store not locked
            result = false;// third store not locked
        }};
        String bucketName = createRandomBucketName();
        String directoryName = createDirectoryStoreName();
        AwsS3SnapshotStore awsS3SnapshotStore = new AwsS3SnapshotStore(m_amazonS3, bucketName, directoryName, m_storeLocker);
        assertThat(awsS3SnapshotStore.listSnapshots()).containsExactly(new BigInteger("123456789012345678901234567890"),//
                                                                       new BigInteger("123456789012345678901234567892"));
    }

    private static String createRandomBucketName() {
        return "iron-bucket-" + UUID.randomUUID();
    }

    private static String createDirectoryStoreName() {
        return "iron-directory-" + UUID.randomUUID();
    }
}
