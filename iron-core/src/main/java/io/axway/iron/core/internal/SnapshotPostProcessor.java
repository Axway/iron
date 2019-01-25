package io.axway.iron.core.internal;

import java.util.function.*;
import com.google.common.annotations.VisibleForTesting;
import io.axway.iron.error.UnrecoverableStoreException;
import io.axway.iron.spi.model.snapshot.SerializableSnapshot;

class SnapshotPostProcessor {

    private BiFunction<SerializableSnapshot, String, SerializableSnapshot> m_processor;
    private long m_beforePostProcessingVersion = -1;
    private long m_afterPostProcessingVersion = -1;

    SnapshotPostProcessor(BiFunction<SerializableSnapshot, String, SerializableSnapshot> processor) {
        m_processor = processor;
    }

    SerializableSnapshot apply(String storeName, SerializableSnapshot serializableSnapshot) {
        m_beforePostProcessingVersion = updateVersionAndCheckConsistency(m_beforePostProcessingVersion, serializableSnapshot.getApplicationModelVersion(),
                                                                         "Snapshot serializable application model version differs among the stores");

        SerializableSnapshot finalSnapshot = m_processor.apply(serializableSnapshot, storeName);
        // consistency check after postProcess
        m_afterPostProcessingVersion = updateVersionAndCheckConsistency(m_afterPostProcessingVersion, finalSnapshot.getApplicationModelVersion(),
                                                                        "Snapshot serializable application model version differs among the stores after post processing");

        if (m_afterPostProcessingVersion < m_beforePostProcessingVersion) {
            throw new UnrecoverableStoreException("Application model version can not be decreased",
                                                  args -> args.add("oldApplicationModelVersion", m_beforePostProcessingVersion)
                                                          .add("newApplicationModelVersion", m_afterPostProcessingVersion));
        }
        return finalSnapshot;
    }

    long getConsistentApplicationModelVersion() {
        long applicationModelVersion = 0;
        if (m_beforePostProcessingVersion > 0) {
            applicationModelVersion = m_beforePostProcessingVersion;
        }
        // since we already validate version after postprocessing no need to validate it again, so if set override the initial version
        if (m_afterPostProcessingVersion > 0) {
            applicationModelVersion = m_afterPostProcessingVersion;
        }
        return applicationModelVersion;
    }

    @VisibleForTesting
    static long updateVersionAndCheckConsistency(long localVersion, long newVersion, String errorMessage) {
        if (localVersion == -1) {
            return newVersion;
        } else {
            if (localVersion != newVersion) {
                throw new UnrecoverableStoreException(errorMessage, args -> args.add("localVersion", localVersion).add("newVersion", newVersion));
            }
        }
        return localVersion;
    }
}
