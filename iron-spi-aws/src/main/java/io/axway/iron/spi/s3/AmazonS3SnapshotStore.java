package io.axway.iron.spi.s3;

import java.io.*;
import java.math.BigInteger;
import java.util.*;
import java.util.regex.*;
import java.util.stream.*;
import javax.annotation.*;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.HeadBucketRequest;
import com.amazonaws.services.s3.model.HeadBucketResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Throwables;
import io.axway.iron.spi.storage.SnapshotStore;

import static com.google.common.base.Preconditions.checkArgument;

class AmazonS3SnapshotStore implements SnapshotStore {
    private static final String DIRNAME_FORMAT = "%s/tx";
    private static final String FILENAME_FORMAT = DIRNAME_FORMAT + "/%020d.tx";
    private static final Pattern FILENAME_PATTERN = Pattern.compile("([0-9]{20})\\.tx");
    private final AmazonS3 m_amazonS3;
    private final String m_bucketName;
    private String m_storeName;

    public AmazonS3SnapshotStore(String accessKey, String secretKey, String bucketName, String storeName, @Nullable String region, @Nullable String s3Endpoint,
                                 @Nullable Long s3Port) {
        m_bucketName = bucketName;
        m_storeName = storeName;

        AWSStaticCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey));

        m_amazonS3 = buildS3Client(credentialsProvider, region, s3Endpoint, s3Port);

        createBucketIfNotExists(bucketName, region);
    }

    private void createBucketIfNotExists(String bucketName, @Nullable String region) {
        HeadBucketRequest headBucketRequest = new HeadBucketRequest(bucketName);
        try {
            HeadBucketResult headBucketResult = m_amazonS3.headBucket(headBucketRequest);
        } catch (AmazonServiceException e) {
            switch (e.getStatusCode()) {
                case 404:
                    CreateBucketRequest createBucketRequest = new CreateBucketRequest(bucketName, region);
                    m_amazonS3.createBucket(createBucketRequest);
                    return;
            }
            throw Throwables.propagate(e);
        }
    }

    private AmazonS3 buildS3Client(AWSStaticCredentialsProvider credentialsProvider, @Nullable String region, @Nullable String s3Endpoint,
                                   @Nullable Long s3Port) {
        checkArgument((region == null && s3Endpoint == null && s3Port == null) || (region != null && s3Endpoint != null && s3Port != null),
                      "region, s3Endpoint and s3Port must all be null or all not null");
        AmazonS3ClientBuilder amazonS3ClientBuilder = AmazonS3ClientBuilder.standard().withCredentials(credentialsProvider);
        amazonS3ClientBuilder.withPathStyleAccessEnabled(true);
        if (region != null && s3Endpoint != null && s3Port != null) {
            String s3EndpointFull = "https://" + s3Endpoint + ":" + s3Port;
            amazonS3ClientBuilder.setEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(s3EndpointFull, region));
        }
        return amazonS3ClientBuilder.build();
    }

    @Override
    public OutputStream createSnapshotWriter(BigInteger transactionId) throws IOException {
        return new ByteArrayOutputStream() {
            @Override
            public void close() throws IOException {
                super.close();
                byte[] content = toByteArray();
                ObjectMetadata metadata = new ObjectMetadata();
                metadata.setContentType("application/json");
                metadata.setContentLength(content.length);
                m_amazonS3.putObject(m_bucketName, getSnapshotFileName(transactionId), new ByteArrayInputStream(content), metadata);
            }
        };
    }

    @Override
    public InputStream createSnapshotReader(BigInteger transactionId) throws IOException {
        S3Object object = m_amazonS3.getObject(m_bucketName, getSnapshotFileName(transactionId));
        if (object == null) {
            throw new RuntimeException(transactionId + " doesn't exists"); //TODO
        }
        return object.getObjectContent();
    }

    @Override
    public List<BigInteger> listSnapshots() {
        return m_amazonS3.listObjectsV2(m_bucketName, getSnapshotDirName()).getObjectSummaries().stream() //
                .map(S3ObjectSummary::getKey).map(FILENAME_PATTERN::matcher) //
                .filter(Matcher::matches) //
                .map(matcher -> matcher.group(1)) //
                .map(BigInteger::new) //
                .collect(Collectors.toList());
    }

    @Override
    public void deleteSnapshot(BigInteger transactionId) {
        m_amazonS3.deleteObject(m_bucketName, getSnapshotFileName(transactionId));
    }

    private String getSnapshotDirName() {
        return String.format(DIRNAME_FORMAT, m_storeName);
    }

    private String getSnapshotFileName(BigInteger id) {
        return String.format(FILENAME_FORMAT, m_storeName, id);
    }
}
