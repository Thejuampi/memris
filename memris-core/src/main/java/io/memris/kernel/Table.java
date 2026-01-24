package io.memris.kernel;

import java.util.Collection;

public interface Table {
    String name();

    long rowCount();

    Column<?> column(String name);

    Collection<Column<?>> columns();
}
