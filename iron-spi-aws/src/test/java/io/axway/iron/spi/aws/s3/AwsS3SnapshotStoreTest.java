package io.axway.iron.spi.aws.s3;

import java.math.BigInteger;
import org.testng.annotations.Test;
import com.amazonaws.services.s3.AmazonS3;
import mockit.Mocked;

import static org.assertj.core.api.Assertions.assertThat;

public class AwsS3SnapshotStoreTest {

    @Mocked
    public AmazonS3 m_amazonS3;

    @Test
    public void shouldReturnRightFileAndDirectoryNames() {
        AwsS3SnapshotStore awsS3SnapshotStore = new AwsS3SnapshotStore(m_amazonS3, "bucketName", "directory");
        //
        String snapshotIdsPrefix = awsS3SnapshotStore.getSnapshotIdsPrefix();
        assertThat(snapshotIdsPrefix).isEqualTo("directory/snapshots/ids/");
        //
        String snapshotDataIdDirectory = awsS3SnapshotStore.getSnapshotDataIdDirectory(BigInteger.valueOf(1234L));
        assertThat(snapshotDataIdDirectory).isEqualTo("directory/snapshots/data/1234");
        //
        String snapshotDataFileName = awsS3SnapshotStore.getSnapshotDataFileName(BigInteger.valueOf(1234L), "storeName");
        assertThat(snapshotDataFileName).isEqualTo("directory/snapshots/data/1234/storeName.snapshot");
        assertThat(awsS3SnapshotStore.extractStoreName(snapshotDataFileName)).containsExactly("storeName");
        //
        String snapshotIdFile = awsS3SnapshotStore.getSnapshotIdFile(BigInteger.valueOf(1234L));
        assertThat(snapshotIdFile).isEqualTo("directory/snapshots/ids/1234");
        assertThat(awsS3SnapshotStore.extractSnapshotId(snapshotIdFile)).containsExactly("1234");
    }
}
