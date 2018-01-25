package io.axway.iron.spi.s3;

import java.math.BigInteger;
import java.util.*;
import org.testng.annotations.Test;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.HeadBucketRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import mockit.Mocked;
import mockit.StrictExpectations;

import static java.util.stream.Stream.*;
import static org.assertj.core.api.Assertions.assertThat;

public class AmazonS3SnapshotStoreTest {

    @Mocked
    AmazonS3 m_amazonS3;

    @Test
    public void shouldCreateS3Bucket() {
        new StrictExpectations() {{
            m_amazonS3.headBucket((HeadBucketRequest) any);
            result = null;
            m_amazonS3.listObjectsV2(anyString, anyString);
            ListObjectsV2Result listObjectsV2Result = new ListObjectsV2Result();
            List<S3ObjectSummary> s3ObjectSummaries = listObjectsV2Result.getObjectSummaries();
            of("123456789012345678901234567890.tx", "123456789012345678901234567891.tx").forEach(name -> {
                S3ObjectSummary s3ObjectSummary = new S3ObjectSummary();
                s3ObjectSummary.setKey(name);
                s3ObjectSummaries.add(s3ObjectSummary);
            });
            result = listObjectsV2Result;
        }};
        AmazonS3SnapshotStore amazonS3SnapshotStore = new AmazonS3SnapshotStore(m_amazonS3, "bucketName", "storeName");
        assertThat(amazonS3SnapshotStore.listSnapshots())
                .containsExactly(new BigInteger("123456789012345678901234567890"), new BigInteger("123456789012345678901234567891"));
    }
}
