package io.axway.iron.core.internal.command;

import java.lang.reflect.Method;
import java.util.*;
import com.google.common.collect.ImmutableMap;
import io.axway.iron.Command;
import io.axway.iron.ReadWriteTransaction;
import io.axway.iron.core.internal.definition.command.CommandDefinition;
import io.axway.iron.core.internal.definition.command.ParameterDefinition;
import io.axway.iron.core.internal.utils.IntrospectionHelper;
import io.axway.iron.core.internal.utils.proxy.ProxyFactory;
import io.axway.iron.core.internal.utils.proxy.ProxyFactoryBuilder;
import io.axway.iron.error.StoreException;
import io.axway.iron.spi.model.transaction.SerializableCommand;

import static io.axway.alf.assertion.Assertion.*;

public class CommandProxyFactory {
    private static final String COMMAND_EXECUTE_METHOD = "execute";

    private static final Method PROXY_COMMAND_CLASS_METHOD = IntrospectionHelper.retrieveMethod(CommandProxy.class, CommandProxy::__commandClass);
    private static final Method PROXY_PARAMETERS_METHOD = IntrospectionHelper.retrieveMethod(CommandProxy.class, CommandProxy::__parameters);

    private final Map<Class<? extends Command<?>>, ProxyFactory<? extends Command<?>, CommandProxyContext>> m_commandProxyFactories;

    public CommandProxyFactory(Collection<CommandDefinition<? extends Command<?>>> commandDefinitions) {
        ImmutableMap.Builder<Class<? extends Command<?>>, ProxyFactory<? extends Command<?>, CommandProxyContext>> commandProxyFactories = ImmutableMap
                .builder();

        commandDefinitions
                .forEach(commandDefinition -> commandProxyFactories.put(commandDefinition.getCommandClass(), createCommandProxyFactory(commandDefinition)));

        m_commandProxyFactories = commandProxyFactories.build();
    }

    private static <C extends Command<?>> ProxyFactory<C, CommandProxyContext> createCommandProxyFactory(CommandDefinition<C> commandDefinition) {
        Class<C> commandClass = commandDefinition.getCommandClass();
        Method executeMethod;
        try {
            executeMethod = commandClass.getDeclaredMethod(COMMAND_EXECUTE_METHOD, ReadWriteTransaction.class);
        } catch (NoSuchMethodException e) {
            throw new StoreException(e);
        }

        checkArgument(executeMethod.isDefault(), "Command execute(ReadWriteTransaction) method has no default implementation",
                      args -> args.add("commandName", commandClass.getName()));

        ProxyFactoryBuilder<CommandProxyContext> builder = ProxyFactoryBuilder.<CommandProxyContext>newProxyFactoryBuilder() //
                .defaultObjectEquals() //
                .defaultObjectHashcode() //
                .handleObjectToString((context, proxy, method, args) -> commandClass.getName() + " " + context.getParameters()) //
                .handle(PROXY_COMMAND_CLASS_METHOD, (context, proxy, method, args) -> commandClass) //
                .handle(PROXY_PARAMETERS_METHOD, (context, proxy, method, args) -> Collections.unmodifiableMap(context.getParameters())) //
                .handleDefaultMethod(executeMethod);

        for (ParameterDefinition<Object> parameterDefinition : commandDefinition.getParameters().values()) {
            builder.handle(parameterDefinition.getParameterMethod(),
                           (context, proxy, method, args) -> parameterDefinition.getTypeConverter().convert(context.getParameters().get(method.getName())));
        }

        return builder //
                .unhandled((context, proxy, method, args) -> context.getParameters().get(method.getName())) //
                .build(commandDefinition.getCommandProxyConstructor());
    }

    public <C extends Command<?>> C createCommand(Class<C> commandClass, Map<String, Object> parameters) {
        //noinspection unchecked
        ProxyFactory<C, CommandProxyContext> commandProxyFactory = (ProxyFactory<C, CommandProxyContext>) m_commandProxyFactories.get(commandClass);
        checkNotNull(commandProxyFactory, "Command is unknown", args -> args.add("commandClass", commandClass.getName()));
        return commandProxyFactory.createProxy(new CommandProxyContext(parameters));
    }

    public String getCommandName(Command<?> command) {
        CommandProxy commandProxy = (CommandProxy) command;
        return commandProxy.__commandClass().getName();
    }

    public SerializableCommand serializeCommand(Command<?> command) {
        CommandProxy commandProxy = (CommandProxy) command;
        SerializableCommand serializableCommand = new SerializableCommand();
        serializableCommand.setCommandName(commandProxy.__commandClass().getName());
        serializableCommand.setParameters(commandProxy.__parameters());
        return serializableCommand;
    }
}
