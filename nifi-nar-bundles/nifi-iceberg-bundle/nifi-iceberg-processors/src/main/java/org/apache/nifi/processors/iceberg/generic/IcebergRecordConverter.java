/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nifi.processors.iceberg.generic;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.commons.lang.Validate;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.schema.SchemaWithPartnerVisitor;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.nifi.processors.iceberg.generic.GenericFieldConverters.binary;
import static org.apache.nifi.processors.iceberg.generic.GenericFieldConverters.booleans;
import static org.apache.nifi.processors.iceberg.generic.GenericFieldConverters.dates;
import static org.apache.nifi.processors.iceberg.generic.GenericFieldConverters.decimals;
import static org.apache.nifi.processors.iceberg.generic.GenericFieldConverters.doubles;
import static org.apache.nifi.processors.iceberg.generic.GenericFieldConverters.fixed;
import static org.apache.nifi.processors.iceberg.generic.GenericFieldConverters.floats;
import static org.apache.nifi.processors.iceberg.generic.GenericFieldConverters.integers;
import static org.apache.nifi.processors.iceberg.generic.GenericFieldConverters.longs;
import static org.apache.nifi.processors.iceberg.generic.GenericFieldConverters.record;
import static org.apache.nifi.processors.iceberg.generic.GenericFieldConverters.strings;
import static org.apache.nifi.processors.iceberg.generic.GenericFieldConverters.times;
import static org.apache.nifi.processors.iceberg.generic.GenericFieldConverters.timestamps;
import static org.apache.nifi.processors.iceberg.generic.GenericFieldConverters.uuids;

public class IcebergRecordConverter {

    private final FieldConverter<Record, org.apache.iceberg.data.Record> converter;

    public org.apache.iceberg.data.Record convert(Record datum) {
        return converter.convert(datum);
    }

    @SuppressWarnings("unchecked")
    public IcebergRecordConverter(Schema schema, RecordSchema recordSchema, FileFormat fileFormat) {
        this.converter = (FieldConverter<Record, org.apache.iceberg.data.Record>) IcebergSchemaVisitor.visit(schema, new RecordDataType(recordSchema), fileFormat);
    }

    private static class IcebergSchemaVisitor extends SchemaWithPartnerVisitor<DataType, FieldConverter<?, ?>> {

        public static FieldConverter<?, ?> visit(Schema schema, RecordDataType recordDataType, FileFormat fileFormat) {
            return visit(schema, new RecordTypeWithFieldNameMapper(schema, recordDataType), new IcebergSchemaVisitor(), new IcebergPartnerAccessors(schema, fileFormat));
        }

        @Override
        public FieldConverter<?, ?> schema(Schema schema, DataType dataType, FieldConverter<?, ?> converter) {
            return converter;
        }

        @Override
        public FieldConverter<?, ?> field(Types.NestedField field, DataType dataType, FieldConverter<?, ?> converter) {
            return converter;
        }

        @Override
        public FieldConverter<?, ?> primitive(Type.PrimitiveType type, DataType dataType) {
            if (type.typeId() != null) {
                switch (type.typeId()) {
                    case BOOLEAN:
                        return booleans();
                    case INTEGER:
                        return integers();
                    case LONG:
                        return longs();
                    case FLOAT:
                        return floats();
                    case DOUBLE:
                        return doubles();
                    case DATE:
                        return dates();
                    case TIME:
                        return times();
                    case TIMESTAMP:
                        Types.TimestampType timestampType = (Types.TimestampType) type;
                        return timestamps(timestampType.shouldAdjustToUTC());
                    case STRING:
                        return strings();
                    case UUID:
                        UUIDDataType uuidType = (UUIDDataType) dataType;
                        return uuids(uuidType.getFileFormat());
                    case FIXED:
                        Types.FixedType fixedType = (Types.FixedType) type;
                        return fixed(fixedType.length());
                    case BINARY:
                        return binary();
                    case DECIMAL:
                        return decimals();
                    default:
                        throw new UnsupportedOperationException("Unsupported type: " + type.typeId());
                }
            }
            return null;
        }

        @Override
        public FieldConverter<?, ?> struct(Types.StructType type, DataType dataType, List<FieldConverter<?, ?>> deserializers) {
            Preconditions.checkNotNull(type, "Can not create reader for null type");
            List<RecordField> recordFields = ((RecordDataType) dataType).getChildSchema().getFields();
            return record(deserializers, recordFields, type);
        }

        @Override
        public FieldConverter<?, ?> list(Types.ListType listTypeInfo, DataType dataType, FieldConverter<?, ?> deserializer) {
            return GenericFieldConverters.array(deserializer, ((ArrayDataType) dataType).getElementType());
        }

        @Override
        public FieldConverter<?, ?> map(Types.MapType mapType, DataType dataType, FieldConverter<?, ?> keyDeserializer, FieldConverter<?, ?> valueDeserializer) {
            return GenericFieldConverters.map(keyDeserializer, RecordFieldType.STRING.getDataType(), valueDeserializer, ((MapDataType) dataType).getValueType());
        }
    }

    public static class IcebergPartnerAccessors implements SchemaWithPartnerVisitor.PartnerAccessors<DataType> {

        private final Schema schema;
        private final FileFormat fileFormat;

        IcebergPartnerAccessors(Schema schema, FileFormat fileFormat) {
            this.schema = schema;
            this.fileFormat = fileFormat;
        }

        @Override
        public DataType fieldPartner(DataType dataType, int fieldId, String name) {
            Validate.isTrue(dataType instanceof RecordTypeWithFieldNameMapper, String.format("Invalid record: %s is not a record", dataType));
            RecordTypeWithFieldNameMapper recordType = (RecordTypeWithFieldNameMapper) dataType;
            Optional<RecordField> recordField = recordType.getChildSchema().getField(recordType.getSourceName(name));

            Validate.isTrue(recordField.isPresent());
            RecordField field = recordField.get();

            // If the actual record contains a nested record them we need to create a field name mapper object for it.
            if (field.getDataType() instanceof RecordDataType) {
                return new RecordTypeWithFieldNameMapper(new Schema(schema.findField(fieldId).type().asStructType().fields()), (RecordDataType) field.getDataType());
            }

            if (field.getDataType().getFieldType().equals(RecordFieldType.UUID)) {
                return new UUIDDataType(field.getDataType(), fileFormat);
            }

            return field.getDataType();
        }

        @Override
        public DataType mapKeyPartner(DataType dataType) {
            return RecordFieldType.STRING.getDataType();
        }

        @Override
        public DataType mapValuePartner(DataType dataType) {
            Validate.isTrue(dataType instanceof MapDataType, String.format("Invalid map: %s is not a map", dataType));
            MapDataType mapType = (MapDataType) dataType;
            return mapType.getValueType();
        }

        @Override
        public DataType listElementPartner(DataType dataType) {
            Validate.isTrue(dataType instanceof ArrayDataType, String.format("Invalid array: %s is not an array", dataType));
            ArrayDataType arrayType = (ArrayDataType) dataType;
            return arrayType.getElementType();
        }
    }

    /**
     * Since the source and the target schema field names can differ despite having the same structure, we create a name mapper to avoid any error coming from the name mismatch.
     */
    private static class RecordTypeWithFieldNameMapper extends RecordDataType {
        private final Map<String, String> sourceNameMap;

        RecordTypeWithFieldNameMapper(Schema schema, RecordDataType recordType) {
            super(recordType.getChildSchema());

            this.sourceNameMap = Maps.newHashMapWithExpectedSize(schema.columns().size());

            List<RecordField> recordFields = recordType.getChildSchema().getFields();
            for (int i = 0; i < schema.columns().size(); ++i) {
                sourceNameMap.put(schema.columns().get(i).name(), recordFields.get(i).getFieldName());
            }
        }

        String getSourceName(String originalName) {
            return sourceNameMap.get(originalName);
        }
    }

    /**
     * Parquet writer expects the UUID value in different format, so it needs to be converted differently: <a href="https://github.com/apache/iceberg/issues/1881">#1881</a>
     */
    private static class UUIDDataType extends DataType {

        private final FileFormat fileFormat;

        UUIDDataType(DataType dataType, FileFormat fileFormat) {
            super(dataType.getFieldType(), dataType.getFormat());
            this.fileFormat = fileFormat;
        }

        public FileFormat getFileFormat() {
            return fileFormat;
        }

    }
}
