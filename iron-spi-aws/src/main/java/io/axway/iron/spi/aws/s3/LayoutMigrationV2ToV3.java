package io.axway.iron.spi.aws.s3;

import java.io.*;
import java.math.BigInteger;
import java.nio.file.Path;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.axway.iron.spi.model.snapshot.SerializableSnapshot;
import io.axway.iron.spi.serializer.SnapshotSerializer;

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
    private static final JacksonSnapshotSerializer SNAPSHOT_SERIALIZER = new JacksonSnapshotSerializer();

    public static void main(String[] args) {
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
            if (args.length > 4) {
                if (args.length != 6) {
                    throw new IllegalArgumentException("Missing arguments");
                }
                endpoint = args[4];
                port = Integer.parseInt(args[5]);
            }
        } catch (Exception e) {
            System.out.println("Usage of Layout Migration : java " + LayoutMigrationV2ToV3.class
                                       + " storeSourceFsDirectory storeDestinationS3Bucket storeDestinationS3Directory [endpoint port]");
            System.out.println("Example: java " + LayoutMigrationV2ToV3.class + " us-east-1 /data/iron bucket iron");
            System.out.println("         java " + LayoutMigrationV2ToV3.class + " us-east-1 /data/iron bucket iron 127.0.0.1 4572");
            throw e;
        }

        AmazonS3 s3Client = buildS3Client(null, null, endpoint, port, region);
        migrateSPiFileStoreToAwsS3Store(storeSourceFsDirectory, storeDestinationS3Bucket, storeDestinationS3Directory, s3Client);
    }

    private static PutObjectRequest forceTransactionIdToZero(PutObjectRequest putObjectRequest) {
        try {
            InputStream inputStream = new BufferedInputStream(new FileInputStream(putObjectRequest.getFile()));
            SerializableSnapshot snapshot = SNAPSHOT_SERIALIZER.deserializeSnapshot("valueNotUsed", inputStream);
            snapshot.setTransactionId(BigInteger.ZERO);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            SNAPSHOT_SERIALIZER.serializeSnapshot(outputStream, snapshot);
            byte[] content = outputStream.toByteArray();
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentType("application/json");
            metadata.setContentLength(content.length);
            InputStream inputStreamModified = new ByteArrayInputStream(content);
            return new PutObjectRequest(putObjectRequest.getBucketName(), putObjectRequest.getKey(), inputStreamModified, metadata);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String[] listDirectories(Path path) {
        return path.toFile().list((dir, name) -> dir.isDirectory());
    }

    private static String[] listSnapshotFiles(Path path) {
        return path.toFile().list((dir, name) -> name.endsWith(".snapshot"));
    }

    private static void migrateSPiFileStoreToAwsS3Store(String storeSourceFsDirectory, String storeDestinationS3Bucket, String storeDestinationS3Directory,
                                                        AmazonS3 s3Client) {
        Path storeSourceFsPath = Path.of(storeSourceFsDirectory);
        String[] stores = listDirectories(storeSourceFsPath);
        for (String store : stores) {
            Path snapshotDirPath = storeSourceFsPath.resolve(store).resolve("snapshot");
            String[] ids = listDirectories(snapshotDirPath);
            String id = ids[ids.length - 1];
            String id0 = "0";
            Path idPath = snapshotDirPath.resolve(id);
            String[] snapshots = listSnapshotFiles(idPath);
            for (String snapshot : snapshots) {
                PutObjectRequest putObjectRequest = new PutObjectRequest(storeDestinationS3Bucket,
                                                                         storeDestinationS3Directory + "/" + store + "/" + "snapshot" + "/" + DATA_DIRECTORY
                                                                                 + "/" + id0 + "/" + snapshot, idPath.resolve(snapshot).toFile());
                s3Client.putObject(forceTransactionIdToZero(putObjectRequest));
            }
            byte[] bytes = "snapshot".getBytes();
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(bytes.length);
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            PutObjectRequest putObjectRequest = new PutObjectRequest(storeDestinationS3Bucket,
                                                                     storeDestinationS3Directory + "/" + store + "/" + "snapshot" + "/" + INDICES_DIRECTORY
                                                                             + "/" + id0, byteArrayInputStream, metadata);
            s3Client.putObject(putObjectRequest);
        }
    }

    private static class JacksonSnapshotSerializer implements SnapshotSerializer {
        private final ObjectMapper m_objectMapper;

        JacksonSnapshotSerializer() {
            m_objectMapper = new ObjectMapper();
            m_objectMapper.configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true);
            m_objectMapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
        }

        @Override
        public void serializeSnapshot(OutputStream out, SerializableSnapshot serializableSnapshot) throws IOException {
            m_objectMapper.writer().writeValues(out).write(serializableSnapshot);
        }

        @Override
        public SerializableSnapshot deserializeSnapshot(String storeName, InputStream in) throws IOException {
            return m_objectMapper.reader().forType(SerializableSnapshot.class).readValue(in);
        }
    }
}

