package io.axway.iron;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import io.axway.iron.functional.Accessor;

/**
 * Permits to query and update the store data.
 */
public interface Store {
    /**
     * Query the store data thanks a {@link ReadOnlyTransaction}.
     *
     * @param storeQuery the store query. The provided {@link ReadOnlyTransaction} must not be used outside the scope of the query method.
     */
    void query(Consumer<ReadOnlyTransaction> storeQuery);

    /**
     * Query the store data thanks a {@link ReadOnlyTransaction} and return a result.
     *
     * @param storeQuery the store query. The provided {@link ReadOnlyTransaction} must not be used outside the scope of the query method.
     * @param <T> the return type of the query
     * @return the result of the {@code storeQuery} function
     */
    <T> T query(Function<ReadOnlyTransaction, T> storeQuery);

    /**
     * Issue a transaction composed of a single command.
     *
     * @param commandClass the class of the command
     * @param <C> the class of command (automatically inferred)
     * @param <T> the return type of the command
     * @return a fluent interface to continue the call
     */
    <C extends Command<T>, T> CommandBuilder<C, T> createCommand(Class<C> commandClass);

    /**
     * Begin a transaction that can be then populated with one or many command.<br>
     * A transaction is atomic, so if one the command fails, all the command of the transaction won't be executed, the updates until the failure will be rollbacked.
     *
     * @return a fluent interface to continue the call
     */
    TransactionBuilder begin();

    interface TransactionBuilder {
        /**
         * Add a new command in the transaction.
         *
         * @param commandClass the class of the command
         * @param <C> the class of command (automatically inferred)
         * @param <T> the return type of the command
         * @return a fluent interface to continue the call
         */
        <C extends Command<T>, T> CommandBuilder<C, T> addCommand(Class<C> commandClass);

        /**
         * Submit the transaction.<br>
         * The transaction commands are executed asynchronously. If the commands results are needed, they can be accessed through the returned {@code Future} object.
         *
         * @return a {@code Future} that gives access to the result of all the command, in the same order they have been added in the transaction.
         */
        Future<List<Object>> submit();
    }

    interface CommandBuilder<C extends Command<T>, T> {
        /**
         * Begin the specification of a command parameter
         *
         * @param accessor the parameter accessor method reference
         * @param <V> the type of value to be set
         * @return a fluent interface to continue the call
         */
        <V> CommandBuilderValueSetter<C, T, V> set(Accessor<C, V> accessor);

        CommandBuilder<C, T> map(Object parameters);

        /**
         * Add the command to the transaction, or if it's a single command transaction submit the transaction with this command.<br>
         * The transaction commands are executed asynchronously. If the command result is needed, it can be accessed through the returned {@code Future} object.
         *
         * @return a {@code Future} that gives access to the result of the command
         */
        Future<T> submit();
    }

    interface CommandBuilderValueSetter<C extends Command<T>, T, V> {
        /**
         * Specify the command parameter value.
         *
         * @param value the value of the parameter
         * @return a fluent interface to continue the call
         */
        CommandBuilder<C, T> to(V value);
    }
}
