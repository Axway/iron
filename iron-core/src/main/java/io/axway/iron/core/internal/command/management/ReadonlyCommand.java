package io.axway.iron.core.internal.command.management;

import javax.xml.bind.annotation.*;
import io.axway.iron.Command;
import io.axway.iron.ReadWriteTransaction;

public interface ReadonlyCommand extends Command<Void> {
    String READONLY_PARAMETER_NAME = "readonly";

    @XmlAttribute(name = READONLY_PARAMETER_NAME)
    boolean readonly();

    @Override
    default Void execute(ReadWriteTransaction tx) {
        return null;
    }
}
