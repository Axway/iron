package io.axway.iron.spi.aws;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.*;
import org.testng.annotations.Test;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import io.axway.iron.spi.aws.s3.LayoutMigrationV2ToV3;

import static io.axway.iron.spi.aws.s3.LayoutMigrationV2ToV3.getPutRequests;
import static io.axway.iron.spi.file.FileTestHelper.copyResource;
import static org.assertj.core.api.Assertions.assertThat;

public class LayoutMigrationV2ToV3IT extends BaseInttest {

    protected final Properties m_configuration = loadConfiguration("configuration.properties");

    @Test(enabled = true)
    public void shouldPutRequestsReturnTheRightRequests() throws IOException {
        try {
            String storeSourceFsDirectory = buildIronS3SnapshotStore();
            List<PutObjectRequest> putRequests = getPutRequests(storeSourceFsDirectory, "myBucket", "myS3Dir1/myS3Dir2");
            assertThat(putRequests.stream().map(putRequest -> putRequest.getBucketName() + "/" + putRequest.getKey())).//
                    containsExactly("myBucket/myS3Dir1/myS3Dir2/global/snapshot/data/00000000000000000000/global.snapshot",
                                    "myBucket/myS3Dir1/myS3Dir2/global/snapshot/ids/00000000000000000000",
                                    "myBucket/myS3Dir1/myS3Dir2/tenant/snapshot/data/00000000000000000000/0.snapshot",
                                    "myBucket/myS3Dir1/myS3Dir2/tenant/snapshot/data/00000000000000000000/1.snapshot",
                                    "myBucket/myS3Dir1/myS3Dir2/tenant/snapshot/data/00000000000000000000/2.snapshot",
                                    "myBucket/myS3Dir1/myS3Dir2/tenant/snapshot/ids/00000000000000000000",
                                    "myBucket/myS3Dir1/myS3Dir2/tenant/snapshot/data/00000000000000000001/0.snapshot",
                                    "myBucket/myS3Dir1/myS3Dir2/tenant/snapshot/data/00000000000000000001/1.snapshot",
                                    "myBucket/myS3Dir1/myS3Dir2/tenant/snapshot/data/00000000000000000001/2.snapshot",
                                    "myBucket/myS3Dir1/myS3Dir2/tenant/snapshot/ids/00000000000000000001");
        } finally {
            //FIXME clean dir
        }
    }

    @Test(enabled = true)
    public void should() throws Exception {
        String storeDestinationS3Bucket = "mybucket";
        String storeDestinationS3Directory = "myDirectory";
        try {
            String storeSourceFsDirectory = buildIronS3SnapshotStore();
            createS3Bucket(storeDestinationS3Bucket);
            LayoutMigrationV2ToV3
                    .main(new String[]{"eu-west-1", storeSourceFsDirectory, storeDestinationS3Bucket, storeDestinationS3Directory, "127.0.0.1", "4572"});
            AmazonS3 s3Client = AwsTestHelper.buildS3Client(m_configuration);
            List<String> keys = s3Client.listObjects(storeDestinationS3Bucket, storeDestinationS3Directory).getObjectSummaries().stream()
                    .map(S3ObjectSummary::getKey).collect(Collectors.toList());
            assertThat(keys).containsExactly("myDirectory/global/snapshot/data/00000000000000000000/global.snapshot",
                                             "myDirectory/global/snapshot/ids/00000000000000000000",
                                             "myDirectory/tenant/snapshot/data/00000000000000000000/0.snapshot",
                                             "myDirectory/tenant/snapshot/data/00000000000000000000/1.snapshot",
                                             "myDirectory/tenant/snapshot/data/00000000000000000000/2.snapshot",
                                             "myDirectory/tenant/snapshot/data/00000000000000000001/0.snapshot",
                                             "myDirectory/tenant/snapshot/data/00000000000000000001/1.snapshot",
                                             "myDirectory/tenant/snapshot/data/00000000000000000001/2.snapshot",
                                             "myDirectory/tenant/snapshot/ids/00000000000000000000", "myDirectory/tenant/snapshot/ids/00000000000000000001");
        } finally {
            deleteS3Bucket(storeDestinationS3Bucket);
        }
    }

    private String buildIronS3SnapshotStore() throws IOException {
        Path randomPath = Paths.get("tmp-iron-test", "iron-spi-file-inttest", "LayoutMigrationV2ToV3IT", UUID.randomUUID().toString());
        copyResource(randomPath, "io", "axway", "iron", "spi", "file", "ironLayoutV3", "iron", "global", "snapshot", "00000000000000000000", "global.snapshot");
        copyResource(randomPath, "io", "axway", "iron", "spi", "file", "ironLayoutV3", "iron", "tenant", "snapshot", "00000000000000000000", "0.snapshot");
        copyResource(randomPath, "io", "axway", "iron", "spi", "file", "ironLayoutV3", "iron", "tenant", "snapshot", "00000000000000000000", "1.snapshot");
        copyResource(randomPath, "io", "axway", "iron", "spi", "file", "ironLayoutV3", "iron", "tenant", "snapshot", "00000000000000000000", "2.snapshot");
        copyResource(randomPath, "io", "axway", "iron", "spi", "file", "ironLayoutV3", "iron", "tenant", "snapshot", "00000000000000000001", "0.snapshot");
        copyResource(randomPath, "io", "axway", "iron", "spi", "file", "ironLayoutV3", "iron", "tenant", "snapshot", "00000000000000000001", "1.snapshot");
        copyResource(randomPath, "io", "axway", "iron", "spi", "file", "ironLayoutV3", "iron", "tenant", "snapshot", "00000000000000000001", "2.snapshot");
        return Paths.get(randomPath.toString(), "io", "axway", "iron", "spi", "file", "ironLayoutV3", "iron").toString();
    }
}
