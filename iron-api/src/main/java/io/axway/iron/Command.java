package io.axway.iron;

/**
 * The parent interface that must be extended by any command.<br>
 *
 * @param <T> the type of the command return value. If no return value is expected, use {@code Void}
 */
public interface Command<T> {
    /**
     * Command subclass must define a default implementation of this method. The execute method is called by the engine
     * when no other reads or no other modification (through a command) is being done.
     *
     * @param tx the {@link ReadWriteTransaction} that can be used to access and modify the model
     * @return the return value, or {@code null} if no return value is expected
     */
    T execute(ReadWriteTransaction tx);
}
