package io.axway.iron.spi.s3;

import org.testng.annotations.Test;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.HeadBucketRequest;
import com.amazonaws.services.s3.model.HeadBucketResult;
import mockit.Mocked;
import mockit.StrictExpectations;

import static org.assertj.core.api.Assertions.assertThat;

public class AwsS3UtilsTest {

    @Mocked
    AmazonS3 m_amazonS3;

    @Test
    public void shouldFindNoS3Bucket() {
        new StrictExpectations() {{
            m_amazonS3.headBucket((HeadBucketRequest) any);
            AmazonServiceException amazonServiceException = new AmazonServiceException("");
            amazonServiceException.setStatusCode(404);
            result = amazonServiceException;
        }};
        assertThat(AwsS3Utils.isBucketAccessible(m_amazonS3, "bucketName")).isFalse();
    }

    @Test
    public void shouldFindAS3Bucket() {
        new StrictExpectations() {{
            m_amazonS3.headBucket((HeadBucketRequest) any);
            result = new HeadBucketResult();
        }};
        assertThat(AwsS3Utils.isBucketAccessible(m_amazonS3, "bucketName")).isTrue();
    }

    @Test(expectedExceptions = AmazonServiceException.class)
    public void shouldThrowAnExceptionWhenNoS3BucketFound() {
        new StrictExpectations() {{
            m_amazonS3.headBucket((HeadBucketRequest) any);
            AmazonServiceException amazonServiceException = new AmazonServiceException("");
            amazonServiceException.setStatusCode(401);
            result = amazonServiceException;
        }};
        AwsS3Utils.isBucketAccessible(m_amazonS3, "bucketName");
    }
}
