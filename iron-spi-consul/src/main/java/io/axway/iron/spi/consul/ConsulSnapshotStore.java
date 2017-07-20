package io.axway.iron.spi.consul;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import java.util.stream.*;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.axway.iron.spi.storage.SnapshotStore;

import static java.lang.String.format;

/**
 * TODO write a few unit tests of this class methods<br>
 * TODO write tests with embedded consul<br>
 * TODO handle connection timeouts  : try another time before throwing error<br>
 * TODO add logging<br>
 */
class ConsulSnapshotStore implements SnapshotStore {

    private static final String GET_METHOD = "GET";
    private static final String PUT_METHOD = "PUT";
    private static final String DELETE_METHOD = "DELETE";
    private static final String CONTENT_TYPE = "Content-Type";
    private static final String APPLICATION_X_WWW_FORM_URLENCODED = "application/x-www-form-urlencoded";
    private static final String CONSUL_SET = "set";

    private static final int CONSUL_MAX_BASE64_VALUE_SIZE = 512_000;
    private static final String IRON_SNAPSHOTS_BASEPATH = "iron-snapshots";

    private final ObjectMapper m_objectMapper;
    private final String m_consulAddress;
    private final String m_storeName;

    ConsulSnapshotStore(ObjectMapper objectMapper, String consulAddress, String storeName) {
        m_objectMapper = objectMapper;
        m_consulAddress = consulAddress;
        m_storeName = storeName;
    }

    @Override
    public OutputStream createSnapshotWriter(long transactionId) throws IOException {
        return new ByteArrayOutputStream() {
            @Override
            public void close() throws IOException {
                super.close();
                byte[] snapshot = toByteArray();
                int consulMaxValueSize = 3 * CONSUL_MAX_BASE64_VALUE_SIZE / 4;
                long chunksCount = (long) Math.ceil((double) snapshot.length / consulMaxValueSize);
                List<ConsulTransactionElement> consulTransaction = new ArrayList<>();
                for (int i = 0; i < chunksCount; i++) {
                    ConsulOperation consulOperation = new ConsulOperation();
                    consulOperation.setVerb(CONSUL_SET);
                    consulOperation.setKey(format("%s/%s/%d/%d", IRON_SNAPSHOTS_BASEPATH, m_storeName, transactionId, i));
                    byte[] decodedChunk = Arrays
                            .copyOfRange(snapshot, i * consulMaxValueSize, Math.min(i * consulMaxValueSize + consulMaxValueSize, snapshot.length));
                    consulOperation.setValue(decodedChunk);
                    ConsulTransactionElement consulTransactionElement = new ConsulTransactionElement();
                    consulTransactionElement.setKV(consulOperation);
                    consulTransaction.add(consulTransactionElement);
                }
                URL url = new URL(format("%s/v1/txn", m_consulAddress));
                HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                connection.setRequestMethod(PUT_METHOD);
                connection.setDoOutput(true);
                try (OutputStream outputStream = connection.getOutputStream()) {
                    m_objectMapper.writeValue(outputStream, consulTransaction);
                }
                int responseCode = connection.getResponseCode();
                if (responseCode != HttpURLConnection.HTTP_OK) {
                    throw new IOException("Snapshot creation error in Consul - response code : " + responseCode);
                }
                //TODO check snapshot is actually in consul because it seems the response code is not trustworthy
            }
        };
    }

    @Override
    public InputStream createSnapshotReader(long transactionId) throws IOException {
        String keyPrefix = format("%s/%s/%d/", IRON_SNAPSHOTS_BASEPATH, m_storeName, transactionId);
        URL url = new URL(format("%s/v1/kv/%s?recurse", m_consulAddress, keyPrefix));
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod(GET_METHOD);
        Set<ConsulKV> chunks;
        try (InputStream in = connection.getInputStream()) {
            chunks = m_objectMapper.readValue(in, new TypeReference<Set<ConsulKV>>() {
            });
        }
        List<byte[]> snapshots = chunks.stream() //
                .sorted(Comparator.comparingLong(s -> Long.parseLong(s.getKey().substring(keyPrefix.length())))) //
                .map(ConsulKV::getValue) //
                .collect(Collectors.toList());

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        for (byte[] snapshot : snapshots) {
            baos.write(snapshot);
        }

        return new ByteArrayInputStream(baos.toByteArray());
    }

    @Override
    public List<Long> listSnapshots() {
        try {
            String keyPrefix = format("%s/%s/", IRON_SNAPSHOTS_BASEPATH, m_storeName);
            URL url = new URL(format("%s/v1/kv/%s/?keys&separator=/", m_consulAddress, keyPrefix));
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod(GET_METHOD);

            Collection<String> keys;
            try (InputStream in = connection.getInputStream()) {
                keys = m_objectMapper.reader().forType(Collection.class).readValue(in);
            }

            return keys.stream().map(key -> Long.parseLong(key.substring(keyPrefix.length(), key.length() - 1))).sorted().collect(Collectors.toList());
        } catch (FileNotFoundException e) {
            return Collections.emptyList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void deleteSnapshot(long transactionId) {
        String urlString = format("%s/v1/kv/%s/%s/%d", m_consulAddress, IRON_SNAPSHOTS_BASEPATH, m_storeName, transactionId);
        try {
            URL url = new URL(urlString);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestProperty(CONTENT_TYPE, APPLICATION_X_WWW_FORM_URLENCODED);
            connection.setRequestMethod(DELETE_METHOD);

            connection.getResponseCode(); // execute the http request
        } catch (FileNotFoundException e) {
            //Key doesn't exist
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
