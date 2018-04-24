package io.axway.iron.core.internal.definition.command;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.axway.iron.Command;
import io.axway.iron.ReadWriteTransaction;
import io.axway.iron.core.internal.command.CommandProxy;
import io.axway.iron.core.internal.definition.DataTypeManager;
import io.axway.iron.core.internal.definition.InterfaceValidator;
import io.axway.iron.core.internal.definition.InterfaceVisitor;
import io.axway.iron.core.internal.utils.proxy.ProxyConstructorFactory;
import io.axway.iron.description.Entity;
import io.axway.iron.description.Id;
import io.axway.iron.description.Unique;
import io.axway.iron.error.InvalidModelException;

public class CommandDefinitionBuilder {
    private static final String EXECUTE_METHOD_NAME = "execute";

    private final ProxyConstructorFactory m_proxyConstructorFactory;
    private final DataTypeManager m_dataTypeManager;
    private final InterfaceValidator m_interfaceValidator;

    public CommandDefinitionBuilder(ProxyConstructorFactory proxyConstructorFactory, DataTypeManager dataTypeManager, InterfaceValidator interfaceValidator) {
        m_proxyConstructorFactory = proxyConstructorFactory;
        m_dataTypeManager = dataTypeManager;
        m_interfaceValidator = interfaceValidator;
    }

    public <C extends Command<?>> CommandDefinition<C> analyzeCommandClass(Class<C> commandClass) {
        ImmutableMap.Builder<String, ParameterDefinition<Object>> parameterDefinitions = ImmutableMap.builder();

        m_interfaceValidator.validate("Command", commandClass, new InterfaceVisitor() {
            @Override
            public void visitInterface(Class<?> clazz) {
                Collection<Class<?>> interfaces = ImmutableList.copyOf(clazz.getInterfaces());
                if (!interfaces.contains(Command.class)) {
                    throw new InvalidModelException("Command class doesn't extends from Command interface",
                                                    args -> args.add("commandClassName", commandClass.getName()));
                }

                Entity entityAnnotation = commandClass.getAnnotation(Entity.class);
                if (entityAnnotation != null) {
                    throw new InvalidModelException("Command must not be annotated with @Entity", args -> args.add("commandClassName", commandClass.getName()));
                }

                if (interfaces.size() > 1) {
                    throw new InvalidModelException("Command extends others interface, which is not supported",
                                                    args -> args.add("commandClassName", commandClass.getName()));
                }

                Method executeMethod;
                try {
                    executeMethod = clazz.getDeclaredMethod(EXECUTE_METHOD_NAME, ReadWriteTransaction.class);
                } catch (NoSuchMethodException ignored) {
                    executeMethod = null;
                }

                if (executeMethod == null) {
                    throw new InvalidModelException("Command doesn't implements method execute(ReadWriteTransaction)",
                                                    args -> args.add("commandClassName", commandClass.getName()));
                }

                if (!executeMethod.isDefault()) {
                    throw new InvalidModelException("Command doesn't provides a default implementation of method execute(ReadWriteTransaction)",
                                                    args -> args.add("commandClassName", commandClass.getName()));
                }
            }

            @Override
            public boolean shouldVisitMethod(Method method) {
                return !(method.getName().equals(EXECUTE_METHOD_NAME) //
                        && method.getParameterCount() == 1 //
                        && method.getParameterTypes()[0].equals(ReadWriteTransaction.class));
            }

            @Override
            public <T> void visitMethod(Method method, Class<T> dataType, boolean multiple, boolean nullable) {
                String methodName = method.getName();

                if (!m_dataTypeManager.isValidDataType(dataType)) {
                    throw new InvalidModelException("Command method data type is not supported", args -> args //
                            .add("commandClassName", commandClass.getName()) //
                            .add("methodName", methodName) //
                            .add("dataTypeName", dataType.getName()));
                }

                Unique uniqueAnnotation = method.getAnnotation(Unique.class);
                if (uniqueAnnotation != null) {
                    throw new InvalidModelException("Command method cannot be annotated with @Unique",
                                                    args -> args.add("commandClassName", commandClass.getName()).add("methodName", methodName));
                }

                Id idAnnotation = method.getAnnotation(Id.class);
                if (idAnnotation != null) {
                    throw new InvalidModelException("Command method cannot be annotated with @Id",
                                                    args -> args.add("commandClassName", commandClass.getName()).add("methodName", methodName));
                }

                ParameterDefinition<?> definition;
                if (multiple) {
                    definition = new ParameterDefinition<>(method, methodName, dataType, true, nullable,
                                                           m_dataTypeManager.getCollectionTypeConverter(dataType));
                } else {
                    definition = new ParameterDefinition<>(method, methodName, dataType, false, nullable, m_dataTypeManager.getTypeConverter(dataType));
                }
                //noinspection unchecked
                parameterDefinitions.put(methodName, (ParameterDefinition<Object>) definition);
            }
        });

        Constructor<C> proxyConstructor = m_proxyConstructorFactory.getProxyConstructor(commandClass, CommandProxy.class);
        return new CommandDefinition<>(commandClass, parameterDefinitions.build(), proxyConstructor);
    }
}
