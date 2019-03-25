package io.axway.iron.spi.aws.s3;

import java.nio.file.Path;
import java.util.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;

import static io.axway.iron.spi.aws.s3.AwsS3Utils.buildS3Client;

/**
 * Layout V2:
 * ironDataStore/global/snapshot/ids/00000
 * ironDataStore/global/snapshot/00000/global.snapshot
 * ironDataStore/global/snapshot/ids/00000
 * ->
 * Layout V3:
 * <ul>
 * <ironDataStore/global/snapshot/ids/00000
 * ironDataStore/global/snapshot/data/00000/global.snapshot
 * </ul>
 */
public class LayoutMigrationV2ToV3 {

    private static final String INDICES_DIRECTORY = "ids";
    private static final String DATA_DIRECTORY = "data";

    public static void main(String[] args) throws Exception {
        String region;
        String storeSourceFsDirectory;
        String storeDestinationS3Bucket;
        String storeDestinationS3Directory;
        String endpoint = null;
        Integer port = null;
        try {
            if (args.length < 4) {
                throw new IllegalArgumentException("Missing arguments");
            }
            if (args.length > 6) {
                throw new IllegalArgumentException("Too many arguments");
            }
            region = args[0];
            storeSourceFsDirectory = args[1];
            storeDestinationS3Bucket = args[2];
            storeDestinationS3Directory = args[3];
            if (args.length > 4 && args.length != 6) {
                throw new IllegalArgumentException("Missing arguments");
            }
            endpoint = args[4];
            port = Integer.parseInt(args[5]);
        } catch (Exception e) {
            System.out.println("Usage of Layout Migration : java " + LayoutMigrationV2ToV3.class
                                       + " storeSourceFsDirectory storeDestinationS3Bucket storeDestinationS3Directory [endpoint port]");
            System.out.println("Example: java " + LayoutMigrationV2ToV3.class + " us-east-1 /data/iron bucket iron");
            System.out.println("         java " + LayoutMigrationV2ToV3.class + " us-east-1 /data/iron bucket iron 127.0.0.1 4572");
            throw e;
        }

        AmazonS3 s3Client = buildS3Client(null, null, endpoint, port, region);
        getPutRequests(storeSourceFsDirectory, storeDestinationS3Bucket, storeDestinationS3Directory).
                forEach(s3Client::putObject);
    }

    private static String[] listDirectories(Path path) {
        return path.toFile().list((dir, name) -> dir.isDirectory());
    }

    private static String[] listSnapshotFiles(Path path) {
        return path.toFile().list((dir, name) -> name.endsWith(".snapshot"));
    }

    public static List<PutObjectRequest> getPutRequests(String storeSourceFsDirectory, String storeDestinationS3Bucket, String storeDestinationS3Directory) {
        List<PutObjectRequest> putObjectRequests = new ArrayList<>();
        Path storeSourceFsPath = Path.of(storeSourceFsDirectory);
        String[] stores = listDirectories(storeSourceFsPath);
        for (String store : stores) {
            Path snapshotDirPath = storeSourceFsPath.resolve(store).resolve("snapshot");
            String[] ids = listDirectories(snapshotDirPath);
            for (String id : ids) {
                Path idPath = snapshotDirPath.resolve(id);
                String[] snapshots = listSnapshotFiles(idPath);
                for (String snapshot : snapshots) {
                    putObjectRequests.add(new PutObjectRequest(storeDestinationS3Bucket,
                                                               storeDestinationS3Directory + "/" + store + "/" + "snapshot" + "/" + DATA_DIRECTORY + "/" + id
                                                                       + "/" + snapshot, idPath.resolve(snapshot).toFile()));
                }
                putObjectRequests.add(new PutObjectRequest(storeDestinationS3Bucket,
                                                           storeDestinationS3Directory + "/" + store + "/" + "snapshot" + "/" + INDICES_DIRECTORY + "/" + id,
                                                           "snapshot"));
            }
        }
        return putObjectRequests;
    }
}

