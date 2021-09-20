package io.axway.iron.spi.kafka;

import io.axway.alf.log.Logger;
import io.axway.alf.log.LoggerFactory;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.server.RunningAsBroker;
import org.apache.kafka.common.utils.Time;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import scala.collection.mutable.ArraySeq;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.axway.alf.assertion.Assertion.checkArgument;
import static io.axway.iron.spi.PortManager.acquireAvailablePort;
import static io.axway.iron.spi.Utils.*;
import static java.lang.String.valueOf;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
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
    static KafkaCluster createStarted(int clusterSize) throws Exception {
        return new KafkaCluster(clusterSize);
    }

    private KafkaCluster(int clusterSize) throws Exception {
        checkArgument(clusterSize > 0 && clusterSize < 10, "Cluster must have a size > 0 and < 10", args -> args.add("clusterSize", clusterSize));

        LOG.info("Starting a new Kafka cluster", args -> args.add("clusterSize", clusterSize));
        long startup = System.currentTimeMillis();

        m_rootPath = createTempDirectory("kafka-cluster-");
        LOG.info("Creating new kafka cluster work dir", arguments -> arguments.add("workDir", m_rootPath));
        m_executorService = Executors.newCachedThreadPool();

        // Start a single zookeeper no matter how many kafka you want
        m_zookeeper = new EmbeddedZookeeper(m_rootPath);

        // Start as many kafka as needed
        List<Callable<EmbeddedKafka>> startupTasks = new ArrayList<>(clusterSize);
        for (int i = 0; i < clusterSize; i++) {
            Path kafkaPath = m_rootPath.resolve("node-" + i);
            startupTasks.add(() -> new EmbeddedKafka(m_zookeeper.getConnectionString(), kafkaPath, clusterSize));
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
    public void close() throws Exception {
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
        private final String m_connectionString;
        private final ServerCnxnFactory m_factory;

        private EmbeddedZookeeper(Path rootPath) {
            try {
                Path m_snapshotDir = createDirectories(rootPath.resolve("zookeeper-snapshot"));
                Path m_logDir = createDirectories(rootPath.resolve("zookeeper-log"));
                final int port = acquireAvailablePort();
                m_connectionString = "localhost:" + port;
                LOG.info("ZK address " + m_connectionString);

                m_factory = ServerCnxnFactory.createFactory(new InetSocketAddress("localhost", port), 1024);
                ZooKeeperServer zkServer = new ZooKeeperServer(new FileTxnSnapLog(m_snapshotDir.toFile(), m_logDir.toFile()), 2000, null);
                m_factory.startup(zkServer);
                while (!zkServer.isRunning()) {
                    sleep(MILLISECONDS, 100);
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
        private final String m_connectionString;
        private final KafkaServer m_broker;

        private EmbeddedKafka(String zookeeperConnectionString, Path rootPath, int clusterSize) {
            m_connectionString = "localhost:" + acquireAvailablePort();
            LOG.info("Kafka address " + m_connectionString);

            Path logDir = createDirectories(rootPath.resolve("kafka-log"));
            Properties kafkaProps = new Properties();
            kafkaProps.setProperty("min.insync.replicas", valueOf(clusterSize / 2 + 1));
            kafkaProps.setProperty("listeners", "PLAINTEXT://" + m_connectionString);
            kafkaProps.setProperty("log.dirs", logDir.toAbsolutePath().toString());
            kafkaProps.setProperty("num.partition", valueOf(clusterSize / 2 + 1));
            kafkaProps.setProperty("zookeeper.connect", zookeeperConnectionString);
            kafkaProps.setProperty("offsets.topic.replication.factor", valueOf(clusterSize / 2 + 1));
            kafkaProps.setProperty("group.initial.rebalance.delay.ms", String.valueOf(0));

            m_broker = new KafkaServer(new KafkaConfig(kafkaProps), Time.SYSTEM, empty(), new ArraySeq<>(0));
            m_broker.startup();
            while (m_broker.brokerState().currentState() != RunningAsBroker.state()) {
                sleep(MILLISECONDS, 100);
            }
        }

        private String getConnectionString() {
            return m_connectionString;
        }

        @Override
        public void close() {
            m_broker.shutdown();
            m_broker.awaitShutdown();
        }
    }
}
