package io.axway.iron.core;

import java.util.*;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import io.axway.iron.core.model.validation.InvalidDatatypeEntity;
import io.axway.iron.core.model.validation.InvalidEmptyEntity;
import io.axway.iron.core.model.validation.InvalidExtendsEntity;
import io.axway.iron.core.model.validation.InvalidIdDefaultEntity;
import io.axway.iron.core.model.validation.InvalidIdMultipleEntity;
import io.axway.iron.core.model.validation.InvalidIdNonLongEntity;
import io.axway.iron.core.model.validation.InvalidIdRelationEntity;
import io.axway.iron.core.model.validation.InvalidIdUniqueEntity;
import io.axway.iron.core.model.validation.InvalidMethodReservedNameEntity;
import io.axway.iron.core.model.validation.InvalidMethodReservedPrefixEntity;
import io.axway.iron.core.model.validation.InvalidMethodVoidEntity;
import io.axway.iron.core.model.validation.InvalidMethodWithArgsEntity;
import io.axway.iron.core.model.validation.InvalidMethodWithDefaultImplementationEntity;
import io.axway.iron.core.model.validation.InvalidMethodWithExceptionsEntity;
import io.axway.iron.core.model.validation.InvalidMissingEntityAnnotationEntity;
import io.axway.iron.core.model.validation.InvalidNonInterfaceEntity;
import io.axway.iron.core.model.validation.InvalidNonnullAndNullableMethodEntity;
import io.axway.iron.core.model.validation.InvalidNonnullPrimitiveWrapperEntity;
import io.axway.iron.core.model.validation.InvalidNullableMultipleRelationEntity;
import io.axway.iron.core.model.validation.InvalidNullablePrimitiveEntity;
import io.axway.iron.core.model.validation.InvalidReverseRelationHeadEntityMismatchEntity;
import io.axway.iron.core.model.validation.InvalidReverseRelationHeadEntityMismatchTargetEntity;
import io.axway.iron.core.model.validation.InvalidReverseRelationMissingDSLCallEntity;
import io.axway.iron.core.model.validation.InvalidReverseRelationMissingDSLCallTargetEntity;
import io.axway.iron.core.model.validation.InvalidReverseRelationNonMultipleEntity;
import io.axway.iron.core.model.validation.InvalidReverseRelationNonMultipleTargetEntity;
import io.axway.iron.core.model.validation.InvalidReverseRelationNullableEntity;
import io.axway.iron.core.model.validation.InvalidReverseRelationNullableTargetEntity;
import io.axway.iron.core.model.validation.InvalidReverseRelationRedundantEntity;
import io.axway.iron.core.model.validation.InvalidReverseRelationRedundantTargetEntity;
import io.axway.iron.core.model.validation.InvalidReverseRelationTailEntityMismatchEntity;
import io.axway.iron.core.model.validation.InvalidReverseRelationTailEntityMismatchTargetEntity;
import io.axway.iron.core.model.validation.InvalidTransientNonImplementedMethodEntity;
import io.axway.iron.core.model.validation.InvalidUniqueRelationEntity;
import io.axway.iron.core.model.validation.TargetEntity;
import io.axway.iron.core.model.validation.TargetWithReverseEntity;
import io.axway.iron.core.model.validation.ValidEntity;
import io.axway.iron.core.model.validation.command.InvalidEntityAnnotationCommand;
import io.axway.iron.core.model.validation.command.InvalidInheritanceCommand;
import io.axway.iron.core.model.validation.command.InvalidMethodIdCommand;
import io.axway.iron.core.model.validation.command.InvalidMethodReservedNameCommand;
import io.axway.iron.core.model.validation.command.InvalidMethodReservedPrefixCommand;
import io.axway.iron.core.model.validation.command.InvalidMethodTypeCommand;
import io.axway.iron.core.model.validation.command.InvalidMethodUniqueCommand;
import io.axway.iron.core.model.validation.command.InvalidMethodVoidCommand;
import io.axway.iron.core.model.validation.command.InvalidMethodWithArgsCommand;
import io.axway.iron.core.model.validation.command.InvalidMethodWithExceptionsCommand;
import io.axway.iron.core.model.validation.command.InvalidMissingExecuteMethodCommand;
import io.axway.iron.core.model.validation.command.InvalidMissingExecuteMethodImplementationCommand;
import io.axway.iron.core.model.validation.command.InvalidNonExtendingCommandCommand;
import io.axway.iron.core.model.validation.command.InvalidNonInterfaceCommand;
import io.axway.iron.core.model.validation.command.InvalidNonnullAndNullableMethodCommand;
import io.axway.iron.core.model.validation.command.InvalidNonnullPrimitiveWrapperCommand;
import io.axway.iron.core.model.validation.command.InvalidNullableCollectionCommand;
import io.axway.iron.core.model.validation.command.InvalidNullablePrimitiveCommand;
import io.axway.iron.core.model.validation.command.InvalidTransientNonImplementedMethodCommand;
import io.axway.iron.core.model.validation.command.ValidCommand;
import io.axway.iron.error.InvalidModelException;
import io.axway.iron.spi.serializer.SnapshotSerializer;
import io.axway.iron.spi.serializer.TransactionSerializer;
import io.axway.iron.spi.storage.SnapshotStore;
import io.axway.iron.spi.storage.TransactionStore;

import static io.axway.iron.core.bugs.IronTestHelper.*;

public class ModelDefinitionValidationTest {

    private StoreManagerBuilder m_builder;

    @BeforeMethod
    public void setUp() {
        SnapshotSerializer snapshotSerializer = buildJacksonSnapshotSerializer();
        TransactionSerializer transactionSerializer = buildJacksonTransactionSerializer();
        SnapshotStore snapshotStore = buildTransientSnapshotStoreFactory();
        TransactionStore transactionStore = buildTransientTransactionStoreFactory();

        m_builder = StoreManagerBuilder.newStoreManagerBuilder() //
                .withSnapshotSerializer(snapshotSerializer) //
                .withTransactionSerializer(transactionSerializer) //
                .withSnapshotStore(snapshotStore) //
                .withTransactionStore(transactionStore);
    }

    @DataProvider(name = "invalidModels")
    public Object[][] providesInvalidModels() throws Exception {
        Class<?> invalidNonPublicEntityClass = getClass().getClassLoader().loadClass("io.axway.iron.core.model.validation.InvalidNonPublicEntity");
        return new Object[][]{ //
                {List.of(InvalidNonInterfaceEntity.class)}, //
                {List.of(invalidNonPublicEntityClass)}, //
                {List.of(InvalidMissingEntityAnnotationEntity.class)}, //
                {List.of(InvalidExtendsEntity.class)}, //
                {List.of(InvalidEmptyEntity.class)}, //
                {List.of(InvalidMethodReservedPrefixEntity.class)}, //
                {List.of(InvalidMethodReservedNameEntity.class)}, //
                {List.of(InvalidTransientNonImplementedMethodEntity.class)}, //
                {List.of(InvalidMethodWithArgsEntity.class)}, //
                {List.of(InvalidMethodWithExceptionsEntity.class)}, //
                {List.of(InvalidMethodVoidEntity.class)}, //
                {List.of(InvalidNonnullAndNullableMethodEntity.class)}, //
                {List.of(InvalidUniqueRelationEntity.class)}, //
                {List.of(InvalidIdDefaultEntity.class)}, //
                {List.of(InvalidIdMultipleEntity.class)}, //
                {List.of(InvalidIdNonLongEntity.class)}, //
                {List.of(InvalidIdRelationEntity.class)}, //
                {List.of(InvalidIdUniqueEntity.class)}, //
                {List.of(InvalidNullableMultipleRelationEntity.class)}, //
                {List.of(InvalidMethodWithDefaultImplementationEntity.class)}, //
                {List.of(InvalidNullablePrimitiveEntity.class)}, //
                {List.of(InvalidNonnullPrimitiveWrapperEntity.class)}, //
                {List.of(InvalidDatatypeEntity.class)}, //
                {List.of(InvalidReverseRelationNonMultipleEntity.class, InvalidReverseRelationNonMultipleTargetEntity.class)}, //
                {List.of(InvalidReverseRelationNullableEntity.class, InvalidReverseRelationNullableTargetEntity.class)}, //
                {List.of(InvalidReverseRelationMissingDSLCallEntity.class, InvalidReverseRelationMissingDSLCallTargetEntity.class)}, //
                {List.of(InvalidReverseRelationTailEntityMismatchEntity.class, InvalidReverseRelationTailEntityMismatchTargetEntity.class)}, //
                {List.of(InvalidReverseRelationHeadEntityMismatchEntity.class, InvalidReverseRelationHeadEntityMismatchTargetEntity.class)}, //
                {List.of(InvalidReverseRelationRedundantEntity.class, InvalidReverseRelationRedundantTargetEntity.class)}, //
        };
    }

    @DataProvider(name = "invalidCommands")
    public Object[][] providesInvalidCommands() throws Exception {
        Class<?> invalidNonPublicCommandClass = getClass().getClassLoader().loadClass("io.axway.iron.core.model.validation.command.InvalidNonPublicCommand");
        return new Object[][]{ //
                {InvalidNonInterfaceCommand.class}, //
                {invalidNonPublicCommandClass}, //
                {InvalidNonExtendingCommandCommand.class}, //
                {InvalidInheritanceCommand.class}, //
                {InvalidMissingExecuteMethodCommand.class}, //
                {InvalidMissingExecuteMethodImplementationCommand.class}, //
                {InvalidMethodReservedNameCommand.class}, //
                {InvalidMethodReservedPrefixCommand.class}, //
                {InvalidNonnullAndNullableMethodCommand.class}, //
                {InvalidNonnullPrimitiveWrapperCommand.class}, //
                {InvalidNullablePrimitiveCommand.class}, //
                {InvalidNullableCollectionCommand.class}, //
                {InvalidTransientNonImplementedMethodCommand.class}, //
                {InvalidMethodWithArgsCommand.class}, //
                {InvalidMethodWithExceptionsCommand.class}, //
                {InvalidMethodVoidCommand.class}, //
                {InvalidMethodIdCommand.class}, //
                {InvalidMethodUniqueCommand.class}, //
                {InvalidEntityAnnotationCommand.class}, //
                {InvalidMethodTypeCommand.class}, //
        };
    }

    @Test(dataProvider = "invalidModels", expectedExceptions = {InvalidModelException.class})
    public void shouldFailOnInvalidModel(Collection<Class<?>> entityClasses) {
        try {
            m_builder.withEntityClass(TargetEntity.class);
            for (Class<?> entityClass : entityClasses) {
                m_builder.withEntityClass(entityClass);
            }
            m_builder.build();
        } catch (InvalidModelException e) {
            System.out.printf("%s => %s%n", entityClasses, e.getMessage());
            throw e;
        }
    }

    @Test
    public void shouldSuccessOnValidModel() {
        m_builder //
                .withEntityClass(ValidEntity.class) //
                .withEntityClass(TargetWithReverseEntity.class) //
                .build();
    }

    @Test(dataProvider = "invalidCommands", expectedExceptions = {InvalidModelException.class})
    public void shouldFailOnInvalidCommand(Class<?> commandClass) {
        try {
            //noinspection unchecked not all classes passed here are compatible with the generic definition. It's the goal of the test to verify such checks are done
            m_builder.withCommandClass((Class) commandClass).build();
        } catch (InvalidModelException e) {
            System.out.printf("%s => %s%n", commandClass, e.getMessage());
            throw e;
        }
    }

    @Test
    public void shouldSuccessOnValidCommand() {
        m_builder.withCommandClass(ValidCommand.class).build();
    }
}
