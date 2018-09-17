package io.axway.iron.spi.aws.s3;

import org.testng.annotations.Test;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.HeadBucketRequest;
import com.amazonaws.services.s3.model.HeadBucketResult;
import mockit.Expectations;
import mockit.Mocked;

public class AwsS3UtilsTest {

    @Mocked
    private AmazonS3 m_amazonS3;

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Bucket is not accessible.*")
    public void shouldFindNoS3Bucket() {
        new Expectations() {{
            m_amazonS3.headBucket((HeadBucketRequest) any);
            AmazonServiceException amazonServiceException = new AmazonServiceException("");
            amazonServiceException.setStatusCode(404);
            result = amazonServiceException;
        }};
        AwsS3Utils.checkBucketIsAccessible(m_amazonS3, "myBucketName");
    }

    @Test
    public void shouldFindAS3Bucket() {
        new Expectations() {{
            m_amazonS3.headBucket((HeadBucketRequest) any);
            result = new HeadBucketResult();
        }};
        AwsS3Utils.checkBucketIsAccessible(m_amazonS3, "myBucketName");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Bucket is not accessible.*")
    public void shouldThrowAnExceptionWhenNoS3BucketFound() {
        new Expectations() {{
            m_amazonS3.headBucket((HeadBucketRequest) any);
            AmazonServiceException amazonServiceException = new AmazonServiceException("");
            amazonServiceException.setStatusCode(401);
            result = amazonServiceException;
        }};
        AwsS3Utils.checkBucketIsAccessible(m_amazonS3, "myBucketName");
    }
}
