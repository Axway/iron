package io.axway.iron.spi.aws.s3;

import org.testng.annotations.Test;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.HeadBucketRequest;
import com.amazonaws.services.s3.model.HeadBucketResult;
import mockit.Mocked;
import mockit.StrictExpectations;

public class AwsS3UtilsTest {

    @Mocked
    AmazonS3 m_amazonS3;

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Bucket bucketName is not accessible.")
    public void shouldFindNoS3Bucket() {
        new StrictExpectations() {{
            m_amazonS3.headBucket((HeadBucketRequest) any);
            AmazonServiceException amazonServiceException = new AmazonServiceException("");
            amazonServiceException.setStatusCode(404);
            result = amazonServiceException;
        }};
        AwsS3Utils.checkBucketIsAccessible(m_amazonS3, "bucketName");
    }

    @Test
    public void shouldFindAS3Bucket() {
        new StrictExpectations() {{
            m_amazonS3.headBucket((HeadBucketRequest) any);
            result = new HeadBucketResult();
        }};
        AwsS3Utils.checkBucketIsAccessible(m_amazonS3, "bucketName");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Bucket bucketName is not accessible.")
    public void shouldThrowAnExceptionWhenNoS3BucketFound() {
        new StrictExpectations() {{
            m_amazonS3.headBucket((HeadBucketRequest) any);
            AmazonServiceException amazonServiceException = new AmazonServiceException("");
            amazonServiceException.setStatusCode(401);
            result = amazonServiceException;
        }};
        AwsS3Utils.checkBucketIsAccessible(m_amazonS3, "bucketName");
    }
}
