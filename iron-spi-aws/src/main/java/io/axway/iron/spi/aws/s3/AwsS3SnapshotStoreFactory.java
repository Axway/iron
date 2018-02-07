package io.axway.iron.spi.aws.s3;

import java.util.*;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import io.axway.iron.spi.storage.SnapshotStore;
import io.axway.iron.spi.storage.SnapshotStoreFactory;

import static io.axway.iron.spi.aws.PropertiesHelper.*;

public class AwsS3SnapshotStoreFactory implements SnapshotStoreFactory {

    private final String m_bucketName;
    private final AmazonS3 m_amazonS3;

    public AwsS3SnapshotStoreFactory(Properties properties) {
        String bucketName = checkKeyHasValue(properties, BUCKET_NAME_KEY);
        String accessKey = checkKeyHasValue(properties, ACCESS_KEY_KEY);
        String secretKey = checkKeyHasValue(properties, SECRET_KEY_KEY);
        String region = checkKeyHasValue(properties, REGION_KEY);
        String s3Endpoint = checkKeyHasValue(properties, S3_ENDPOINT_KEY);
        Long s3Port = checkKeyHasLongValue(properties, S3_PORT_KEY);

        AWSStaticCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey));
        m_amazonS3 = AwsS3Utils.buildS3Client(credentialsProvider, region, s3Endpoint, s3Port);
        m_bucketName = bucketName;
    }

    @Override
    public SnapshotStore createSnapshotStore(String storeName) {
        return new AwsS3SnapshotStore(m_amazonS3, m_bucketName, storeName);
    }
}
