package io.axway.iron.spi.kafka;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import org.apache.kafka.common.utils.Time;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import io.axway.alf.log.Logger;
import io.axway.alf.log.LoggerFactory;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.server.RunningAsBroker;
import scala.collection.mutable.ArraySeq;

import static io.axway.alf.assertion.Assertion.checkArgument;
import static io.axway.iron.spi.kafka.Utils.*;
import static java.lang.String.valueOf;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.*;
import static java.util.stream.Collectors.*;
import static scala.Option.empty;

/**
 * This class is an embedded Kafka cluster. It always starts one Zookeeper but can start as many Kafka as you want.
 */
final class KafkaCluster implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaCluster.class);

    private final Path m_rootPath;
    private final ExecutorService m_executorService;
    private final EmbeddedZookeeper m_zookeeper;
    private final List<EmbeddedKafka> m_embeddedKafkas;
    private final String m_connectionString;

    /**
     * Creates a started Kafka cluster with the given number of nodes.
     *
     * @param clusterSize number of nodes in the Kafka cluster
     * @return {@link KafkaCluster} instance
     */
    static KafkaCluster createStarted(int clusterSize) {
        return new KafkaCluster(clusterSize);
    }

    private KafkaCluster(int clusterSize) {
        checkArgument(clusterSize > 0 && clusterSize < 10, "Cluster must have a size > 0 and < 10", args -> args.add("clusterSize", clusterSize));

        LOG.info("Starting a new Kafka cluster", args -> args.add("clusterSize", clusterSize));
        long startup = System.currentTimeMillis();

        m_rootPath = createTempDirectory("kafka-cluster-");
        m_executorService = Executors.newCachedThreadPool();

        // Start a single zookeeper no matter how many kafka you want
        m_zookeeper = new EmbeddedZookeeper(m_rootPath);

        // Start as many kafka as needed
        List<Callable<EmbeddedKafka>> startupTasks = new ArrayList<>(clusterSize);
        for (int i = 0; i < clusterSize; i++) {
            Path kafkaPath = m_rootPath.resolve("node-" + i);
            startupTasks.add(() -> new EmbeddedKafka(kafkaPath, clusterSize));
        }
        m_embeddedKafkas = invokeAll(m_executorService, startupTasks);
        m_connectionString = m_embeddedKafkas.stream().map(EmbeddedKafka::getConnectionString).collect(joining(","));

        long duration = System.currentTimeMillis() - startup;
        LOG.info("Kafka cluster started",
                 args -> args.add("clusterSize", clusterSize).add("startupMillis", duration).add("connectionString", m_connectionString));
    }

    /**
     * Gets the connection string of the Kafka cluster
     *
     * @return Connection string of the Kafka cluster
     */
    String getConnectionString() {
        return m_connectionString;
    }

    @Override
    public void close() {
        // Stop all Kafka nodes
        List<Callable<Void>> shutdownTasks = m_embeddedKafkas.stream().map(kafka -> (Callable<Void>) () -> {
            kafka.close();
            return null;
        }).collect(toList());
        invokeAll(m_executorService, shutdownTasks);
        m_executorService.shutdown();

        // Stop zookeeper
        m_zookeeper.close();

        // Delete directory
        tryDeleteDirectory(m_rootPath);
    }

    private static final class EmbeddedZookeeper implements AutoCloseable {
        private final Path m_snapshotDir;
        private final Path m_logDir;
        private final String m_connectionString;
        private final ServerCnxnFactory m_factory;

        private EmbeddedZookeeper(Path rootPath) {
            try {
                m_snapshotDir = createDirectories(rootPath.resolve("zookeeper-snapshot"));
                m_logDir = createDirectories(rootPath.resolve("zookeeper-log"));
                final int port = providePort();
                m_connectionString = "localhost:" + port;

                m_factory = ServerCnxnFactory.createFactory(new InetSocketAddress("localhost", port), 1024);
                ZooKeeperServer zkServer = new ZooKeeperServer(m_snapshotDir.toFile(), m_logDir.toFile(), 2000);
                m_factory.setMaxClientCnxnsPerHost(0);
                m_factory.startup(zkServer);
                while (!zkServer.isRunning()) {
                    sleep(MILLISECONDS, 20);
                }
            } catch (IOException e) {
                throw new UncheckedIOException("Cannot start the Zookeeper server", e);
            } catch (InterruptedException e) {
                currentThread().interrupt();
                throw new IllegalStateException("Thread was interrupted while starting Zookeeper", e);
            }
        }

        private String getConnectionString() {
            return m_connectionString;
        }

        @Override
        public void close() {
            m_factory.shutdown();
        }
    }

    private static final class EmbeddedKafka implements AutoCloseable {
        private final EmbeddedZookeeper m_zookeeper;
        private final String m_connectionString;
        private final Path m_logDir;
        private final KafkaServer m_broker;

        private EmbeddedKafka(Path rootPath, int clusterSize) {
            // Start zookeeper
            m_zookeeper = new EmbeddedZookeeper(rootPath);

            m_connectionString = "localhost:" + providePort();
            m_logDir = createDirectories(rootPath.resolve("kafka-log"));
            Properties kafkaProps = new Properties();
            kafkaProps.setProperty("min.insync.replicas", valueOf(clusterSize / 2 + 1));
            kafkaProps.setProperty("listeners", "PLAINTEXT://" + m_connectionString);
            kafkaProps.setProperty("log.dirs", m_logDir.toAbsolutePath().toString());
            kafkaProps.setProperty("num.partition", valueOf(clusterSize / 2 + 1));
            kafkaProps.setProperty("zookeeper.connect", m_zookeeper.getConnectionString());
            kafkaProps.setProperty("offsets.topic.replication.factor", valueOf(clusterSize / 2 + 1));
            kafkaProps.setProperty("group.initial.rebalance.delay.ms", String.valueOf(0));

            m_broker = new KafkaServer(new KafkaConfig(kafkaProps), Time.SYSTEM, empty(), new ArraySeq<>(0));
            m_broker.startup();
            while (m_broker.brokerState().currentState() != RunningAsBroker.state()) {
                sleep(MILLISECONDS, 20);
            }
        }

        private String getConnectionString() {
            return m_connectionString;
        }

        @Override
        public void close() {
            m_broker.shutdown();
            m_broker.awaitShutdown();
            m_zookeeper.close();
        }
    }
}
