package io.axway.iron.spi.aws.s3;

import java.util.*;
import com.amazonaws.services.s3.AmazonS3;
import io.axway.iron.spi.storage.SnapshotStore;
import io.axway.iron.spi.storage.SnapshotStoreFactory;

import static com.google.common.base.Preconditions.checkState;
import static io.axway.iron.spi.aws.PropertiesHelper.getValue;
import static io.axway.iron.spi.aws.s3.AwsS3Properties.S3_BUCKET_NAME_KEY;

public class AwsS3SnapshotStoreFactory implements SnapshotStoreFactory {

    private final String m_bucketName;
    private final AmazonS3 m_amazonS3;

    public AwsS3SnapshotStoreFactory(Properties properties) {
        Optional<String> bucketName = getValue(properties, S3_BUCKET_NAME_KEY);
        checkState(bucketName.isPresent() && !bucketName.get().trim().isEmpty());
        m_amazonS3 = AwsS3Utils.buildS3Client(properties);
        m_bucketName = AwsS3Utils.checkBucketIsAccessible(m_amazonS3, bucketName.get());
    }

    @Override
    public SnapshotStore createSnapshotStore(String storeName) {
        return new AwsS3SnapshotStore(m_amazonS3, m_bucketName, storeName);
    }
}
