package io.axway.iron.core.internal;

import java.util.function.*;
import org.testng.annotations.Test;
import io.axway.iron.error.UnrecoverableStoreException;
import io.axway.iron.spi.model.snapshot.SerializableSnapshot;

import static io.axway.iron.core.internal.SnapshotPostProcessor.updateVersionAndCheckConsistency;
import static io.axway.iron.spi.model.snapshot.SerializableSnapshot.SNAPSHOT_MODEL_VERSION;
import static org.assertj.core.api.Assertions.assertThat;

public class SnapshotPostProcessorTest {

    @Test
    public void shouldSucceedWhenInitializionSnapshotToAppModelVersion() {
        assertThat(updateVersionAndCheckConsistency(-1L, 5L, "shouldSucceed because the applicationVersion was not yet initialized"))
                .as("Whenever the Version is not initialized we can set it").isEqualTo(5L);
    }

    @Test(expectedExceptions = UnrecoverableStoreException.class)
    public void shouldFailwhenUpdatingSnapshotToOtherVersion() {
        updateVersionAndCheckConsistency(8L, 88L, "should fail because the applicationModelVersion are not consistent between the stores");
    }

    @Test
    public void shouldReturn0WhenNoApplicationVersionDefined() {
        SnapshotPostProcessor snapshotPostProcessor = new SnapshotPostProcessor((snapshot, storeName) -> snapshot);
        assertThat(snapshotPostProcessor.getConsistentApplicationModelVersion()).isEqualTo(0L);
    }

    @Test
    public void shouldSucceedWhenValidatingPostProcessingIdentity() {
        // Given: a snapshot
        SerializableSnapshot initialSnapshot = new SerializableSnapshot();
        long snapshotModelVersion = 5L;
        initialSnapshot.setApplicationModelVersion(snapshotModelVersion);
        initialSnapshot.setSnapshotModelVersion(SNAPSHOT_MODEL_VERSION);
        SnapshotPostProcessor snapshotPostProcessor = new SnapshotPostProcessor((snapshot, storeName) -> snapshot);
        // When: post processing it
        SerializableSnapshot finalSnapshot = snapshotPostProcessor.apply("shouldSucceedWhenValidatingPostProcessingIdentity", initialSnapshot);

        // Then: the identity post processing should not have change the application model version
        assertThat(snapshotPostProcessor.getConsistentApplicationModelVersion()).isEqualTo(snapshotModelVersion);
        assertThat(finalSnapshot.getApplicationModelVersion()).as("ApplicationModelVersion should not have changed by the post processing")
                .isEqualTo(snapshotModelVersion);
    }

    @Test
    public void shouldSucceedWhenValidatingPostProcessingUpdatingApplicationModelVersion() {
        // Given: a snapshot
        SerializableSnapshot initialSnapshot = new SerializableSnapshot();
        long initialSnapshotModelVersion = 8L;
        long finalSnapshotModelVersion = 9L;
        initialSnapshot.setApplicationModelVersion(initialSnapshotModelVersion);
        initialSnapshot.setSnapshotModelVersion(SNAPSHOT_MODEL_VERSION);

        // When: post processing it
        BiFunction<SerializableSnapshot, String, SerializableSnapshot> updateTheApplicationModelVersion = (snapshot, storeName) -> {
            snapshot.setApplicationModelVersion(finalSnapshotModelVersion);
            return snapshot;
        };

        SnapshotPostProcessor snapshotPostProcessor = new SnapshotPostProcessor(updateTheApplicationModelVersion);
        SerializableSnapshot finalSnapshot = snapshotPostProcessor.apply("shouldSucceedWhenValidatingPostProcessingIdentity", initialSnapshot);

        // Then: the post processing simulate an upgrade of the model should change the application model version
        assertThat(snapshotPostProcessor.getConsistentApplicationModelVersion()).isEqualTo(finalSnapshotModelVersion);
        assertThat(finalSnapshot.getApplicationModelVersion()).as("ApplicationModelVersion should be updated").isEqualTo(finalSnapshotModelVersion);
    }

    @Test(expectedExceptions = UnrecoverableStoreException.class)
    public void shouldFailWhenValidatingPostProcessingUpdatingApplicationModelVersion() {
        // Given: a snapshot
        SerializableSnapshot initialSnapshot = new SerializableSnapshot();
        long initialSnapshotModelVersion = 10L;
        long finalSnapshotModelVersion = 9L;
        initialSnapshot.setApplicationModelVersion(initialSnapshotModelVersion);
        initialSnapshot.setSnapshotModelVersion(SNAPSHOT_MODEL_VERSION);

        // When: post processing it
        BiFunction<SerializableSnapshot, String, SerializableSnapshot> updateTheApplicationModelVersion = (snapshot, storeName) -> {
            snapshot.setApplicationModelVersion(finalSnapshotModelVersion);
            return snapshot;
        };
        SnapshotPostProcessor snapshotPostProcessor = new SnapshotPostProcessor(updateTheApplicationModelVersion);

        // Then: the post processing should fail because decrease the application model version
        snapshotPostProcessor.apply("shouldSucceedWhenValidatingPostProcessingIdentity", initialSnapshot);
    }
}
