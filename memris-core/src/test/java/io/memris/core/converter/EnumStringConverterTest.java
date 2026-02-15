package io.memris.core.converter;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class EnumStringConverterTest {

    private enum Status {
        NEW,
        DONE
    }

    @Test
    void shouldConvertEnumValuesToAndFromStorage() {
        var converter = new EnumStringConverter<Status>(Status.class);

        assertThat(converter.javaType()).isEqualTo(Status.class);
        assertThat(converter.storageType()).isEqualTo(String.class);
        assertThat(converter.toStorage(Status.DONE)).isEqualTo("DONE");
        assertThat(converter.fromStorage("NEW")).isEqualTo(Status.NEW);
    }

    @Test
    void shouldHandleNullValues() {
        var converter = new EnumStringConverter<Status>(Status.class);

        assertThat(converter.toStorage(null)).isNull();
        assertThat(converter.fromStorage(null)).isNull();
    }

    @Test
    void typeConverterDefaultColumnNameShouldReturnFieldName() {
        TypeConverter<Integer, Integer> converter = new TypeConverter<>() {
            @Override
            public Class<Integer> javaType() {
                return Integer.class;
            }

            @Override
            public Class<Integer> storageType() {
                return Integer.class;
            }

            @Override
            public Integer toStorage(Integer javaValue) {
                return javaValue;
            }

            @Override
            public Integer fromStorage(Integer storageValue) {
                return storageValue;
            }
        };

        assertThat(converter.getColumnName("stock")).isEqualTo("stock");
    }
}
