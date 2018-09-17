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
    private AmazonS3 m_amazonS3;

    @Test
    public void shouldCreateS3Bucket() {
        new Expectations() {{
            m_amazonS3.listObjectsV2((ListObjectsV2Request) any).getCommonPrefixes();
            result = Arrays.asList("blabla/snapshot/123456789012345678901234567890/",     //
                                   "blabla/snapshot/123456789012345678901234567891/");
        }};
        String bucketName = createRandomBucketName();
        String directoryName = createDirectoryStoreName();
        AwsS3SnapshotStore awsS3SnapshotStore = new AwsS3SnapshotStore(m_amazonS3, bucketName, directoryName);
        assertThat(awsS3SnapshotStore.listSnapshots())
                .containsExactly(new BigInteger("123456789012345678901234567890"), new BigInteger("123456789012345678901234567891"));
    }

    private static String createRandomBucketName() {
        return "iron-bucket-" + UUID.randomUUID();
    }

    private static String createDirectoryStoreName() {
        return "iron-directory-" + UUID.randomUUID();
    }
}
