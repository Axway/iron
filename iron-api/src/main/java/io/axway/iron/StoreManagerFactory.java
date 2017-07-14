package io.axway.iron;

import java.util.regex.*;

/**
 * A store manager factory permit to open many stores sharing the same model definition but not the same model instances.
 */
public interface StoreManagerFactory {
    /**
     * Any store name used in the {@link #openStore(String)} method must comply with this pattern.
     */
    Pattern STORE_NAME_VALIDATOR_PATTERN = Pattern.compile("[a-zA-Z0-9\\-_]+");

    /**
     * Open a store. If the store already exists it's recovered from the latest snapshot and from the command redolog. Else a new store is created.
     *
     * @param storeName the name of the store
     * @return the {@link StoreManager} that permits to access to this store.
     */
    StoreManager openStore(String storeName);
}
