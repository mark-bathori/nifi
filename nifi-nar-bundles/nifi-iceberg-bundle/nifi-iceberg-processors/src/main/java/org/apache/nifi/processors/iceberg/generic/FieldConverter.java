package org.apache.nifi.processors.iceberg.generic;

public interface FieldConverter<D, T> {
    T convert(D datum);
}
