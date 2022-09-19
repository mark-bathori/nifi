package org.apache.nifi.processors.iceberg.generic;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.Validate;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;
import org.apache.nifi.processors.iceberg.appender.ArrayElementGetter;
import org.apache.nifi.processors.iceberg.appender.RecordFieldGetter;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.nifi.processors.iceberg.appender.RecordFieldGetter.createFieldGetter;

public class GenericFieldConverters {

    static FieldConverter<Boolean, Boolean> booleans() {
        return BooleanConverter.INSTANCE;
    }

    static FieldConverter<Integer, Integer> integers() {
        return IntegerConverter.INSTANCE;
    }

    static FieldConverter<Long, Long> longs() {
        return LongConverter.INSTANCE;
    }

    static FieldConverter<Float, Float> floats() {
        return FloatConverter.INSTANCE;
    }

    static FieldConverter<Double, Double> doubles() {
        return DoubleConverter.INSTANCE;
    }

    static FieldConverter<LocalDate, LocalDate> dates() {
        return DateConverter.INSTANCE;
    }

    static FieldConverter<Time, LocalTime> times() {
        return TimeConverter.INSTANCE;
    }

    static FieldConverter<Timestamp, ?> timestamps(boolean withTimezone) {
        return withTimezone ? TimestampWithTimezoneConverter.INSTANCE : TimestampConverter.INSTANCE;
    }

    static FieldConverter<String, String> strings() {
        return StringConverter.INSTANCE;
    }

    static FieldConverter<UUID, ?> uuids(FileFormat fileFormat) {
        if(fileFormat.equals(FileFormat.PARQUET)) {
            return UUIDtoByteArrayConverter.INSTANCE;
        }
        return UUIDConverter.INSTANCE;
    }

    static FieldConverter<Byte[], byte[]> fixed(int length) {
        return new FixedConverter(length);
    }

    static FieldConverter<Byte[], ByteBuffer> binary() {
        return BinaryConverter.INSTANCE;
    }

    static FieldConverter<BigDecimal, BigDecimal> decimals() {
        return DecimalConverter.INSTANCE;
    }

    static <T, S> FieldConverter<T[], List<S>> array(FieldConverter<T, S> elementConverter, DataType dataType) {
        return new ArrayConverter<>(elementConverter, dataType);
    }

    static <K, V, L, B> FieldConverter<Map<K, V>, Map<L, B>> map(FieldConverter<K, L> keyConverter, DataType keyType, FieldConverter<V, B> valueConverter, DataType valueType) {
        return new MapConverter<>(keyConverter, keyType, valueConverter, valueType);
    }

    static FieldConverter<Record, org.apache.iceberg.data.Record> record(List<FieldConverter<?, ?>> converters, List<RecordField> recordFields, Types.StructType schema) {
        return new RecordConverter(converters, recordFields, schema);
    }

    private static class BooleanConverter implements FieldConverter<Boolean, Boolean> {

        private static final BooleanConverter INSTANCE = new BooleanConverter();

        @Override
        public Boolean convert(Boolean datum) {
            return datum;
        }
    }

    private static class IntegerConverter implements FieldConverter<Integer, Integer> {

        private static final IntegerConverter INSTANCE = new IntegerConverter();

        @Override
        public Integer convert(Integer datum) {
            return datum;
        }
    }

    private static class LongConverter implements FieldConverter<Long, Long> {

        private static final LongConverter INSTANCE = new LongConverter();

        @Override
        public Long convert(Long datum) {
            return datum;
        }
    }

    private static class FloatConverter implements FieldConverter<Float, Float> {

        private static final FloatConverter INSTANCE = new FloatConverter();

        @Override
        public Float convert(Float datum) {
            return datum;
        }
    }

    private static class DoubleConverter implements FieldConverter<Double, Double> {

        private static final DoubleConverter INSTANCE = new DoubleConverter();

        @Override
        public Double convert(Double datum) {
            return datum;
        }
    }

    private static class DateConverter implements FieldConverter<LocalDate, LocalDate> {

        private static final DateConverter INSTANCE = new DateConverter();

        @Override
        public LocalDate convert(LocalDate datum) {
            return datum;
        }
    }

    private static class TimeConverter implements FieldConverter<Time, LocalTime> {

        private static final TimeConverter INSTANCE = new TimeConverter();

        @Override
        public LocalTime convert(Time datum) {
            return datum.toLocalTime();
        }
    }

    private static class TimestampConverter implements FieldConverter<Timestamp, LocalDateTime> {

        private static final TimestampConverter INSTANCE = new TimestampConverter();

        @Override
        public LocalDateTime convert(Timestamp datum) {
            return datum.toLocalDateTime();
        }
    }

    private static class TimestampWithTimezoneConverter implements FieldConverter<Timestamp, OffsetDateTime> {

        private static final TimestampWithTimezoneConverter INSTANCE = new TimestampWithTimezoneConverter();

        @Override
        public OffsetDateTime convert(Timestamp datum) {
            return OffsetDateTime.ofInstant(datum.toInstant(), ZoneId.of("UTC"));
        }
    }

    private static class StringConverter implements FieldConverter<String, String> {

        private static final StringConverter INSTANCE = new StringConverter();

        @Override
        public String convert(String datum) {
            return datum;
        }
    }

    private static class UUIDtoByteArrayConverter implements FieldConverter<UUID, byte[]> {

        private static final UUIDtoByteArrayConverter INSTANCE = new UUIDtoByteArrayConverter();

        @Override
        public byte[] convert(UUID datum) {
            ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[16]);
            byteBuffer.putLong(datum.getMostSignificantBits());
            byteBuffer.putLong(datum.getLeastSignificantBits());
            return byteBuffer.array();
        }
    }

    private static class UUIDConverter implements FieldConverter<UUID, UUID> {

        private static final UUIDConverter INSTANCE = new UUIDConverter();

        @Override
        public UUID convert(UUID datum) {
            return datum;
        }
    }

    private static class FixedConverter implements FieldConverter<Byte[], byte[]> {

        private final int length;

        FixedConverter(int length) {
            this.length = length;
        }

        @Override
        public byte[] convert(Byte[] datum) {
            Validate.isTrue(datum.length == length);
            return ArrayUtils.toPrimitive(datum);
        }
    }

    private static class BinaryConverter implements FieldConverter<Byte[], ByteBuffer> {

        private static final BinaryConverter INSTANCE = new BinaryConverter();

        @Override
        public ByteBuffer convert(Byte[] datum) {
            return ByteBuffer.wrap(ArrayUtils.toPrimitive(datum));
        }
    }

    private static class DecimalConverter implements FieldConverter<BigDecimal, BigDecimal> {

        private static final DecimalConverter INSTANCE = new DecimalConverter();

        @Override
        public BigDecimal convert(BigDecimal datum) {

//            Preconditions.checkArgument(data.scale() == scale,
//                    "Cannot write value as decimal(%s,%s), wrong scale: %s", precision, scale, data);
//            Preconditions.checkArgument(data.precision() <= precision,
//                    "Cannot write value as decimal(%s,%s), invalid precision: %s", precision, scale, data);
            return datum;
        }
    }

    private static class ArrayConverter<T, S> implements FieldConverter<T[], List<S>> {
        private final FieldConverter<T, S> fieldConverter;
        private final ArrayElementGetter.ElementGetter elementGetter;

        private ArrayConverter(FieldConverter<T, S> elementConverter, DataType dataType) {
            this.fieldConverter = elementConverter;
            this.elementGetter = ArrayElementGetter.createElementGetter(dataType);
        }

        @Override
        @SuppressWarnings("unchecked")
        public List<S> convert(T[] array) {
            final int numElements = array.length;
            List<S> result = new ArrayList<>(numElements);
            for (int i = 0; i < numElements; i += 1) {
                result.add(i, fieldConverter.convert((T) elementGetter.getElementOrNull(array, i)));
            }
            return result;
        }
    }

    private static class MapConverter<K, V, L, B> implements FieldConverter<Map<K, V>, Map<L, B>> {
        private final FieldConverter<K, L> keyConverter;
        private final FieldConverter<V, B> valueConverter;
        private final ArrayElementGetter.ElementGetter keyGetter;
        private final ArrayElementGetter.ElementGetter valueGetter;

        private MapConverter(FieldConverter<K, L> keyConverter, DataType keyType, FieldConverter<V, B> valueConverter, DataType valueType) {
            this.keyConverter = keyConverter;
            this.keyGetter = ArrayElementGetter.createElementGetter(keyType);
            this.valueConverter = valueConverter;
            this.valueGetter = ArrayElementGetter.createElementGetter(valueType);
        }

        @Override
        @SuppressWarnings("unchecked")
        public Map<L, B> convert(Map<K, V> map) {
            final int mapSize = map.size();
            final Object[] keyArray = map.keySet().toArray();
            final Object[] valueArray = map.values().toArray();
            Map<L, B> result = new HashMap<>(mapSize);
            for (int i = 0; i < mapSize; i += 1) {
                result.put(keyConverter.convert((K) keyGetter.getElementOrNull(keyArray, i)), valueConverter.convert((V) valueGetter.getElementOrNull(valueArray, i)));
            }

            return result;
        }
    }

    private static class RecordConverter implements FieldConverter<Record, org.apache.iceberg.data.Record> {

        private final FieldConverter<?, ?>[] converters;
        private final RecordFieldGetter.FieldGetter[] getters;

        private final Types.StructType schema;

        private RecordConverter(List<FieldConverter<?, ?>> converters, List<RecordField> recordFields, Types.StructType schema) {
            this.schema = schema;
            this.converters = (FieldConverter<?, ?>[]) Array.newInstance(FieldConverter.class, converters.size());
            this.getters = new RecordFieldGetter.FieldGetter[converters.size()];
            for (int i = 0; i < converters.size(); i += 1) {
                final RecordField recordField = recordFields.get(i);
                this.converters[i] = converters.get(i);
                this.getters[i] = createFieldGetter(recordField.getDataType(), recordField.getFieldName(), recordField.isNullable());
            }
        }

        @Override
        public org.apache.iceberg.data.Record convert(Record record) {

            GenericRecord template = GenericRecord.create(schema);
            // GenericRecord.copy() is more performant then GenericRecord.create(StructType) since NAME_MAP_CACHE access is eliminated. Using copy here to gain performance.
            org.apache.iceberg.data.Record result = template.copy();

            for (int i = 0; i < converters.length; i += 1) {
                result.set(i, convert(record, i, converters[i]));
            }

            return result;
        }

        @SuppressWarnings("unchecked")
        private <T, S> S convert(Record record, int pos, FieldConverter<T, S> converter) {
            return converter.convert((T) getters[pos].getFieldOrNull(record));
        }
    }
}
