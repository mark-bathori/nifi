/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.common.zendesk.util;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.type.ChoiceDataType;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.stream.Collectors.toList;
import static org.apache.nifi.common.zendesk.ZendeskProperties.ZENDESK_TICKETS_ROOT_NODE;
import static org.apache.nifi.common.zendesk.ZendeskProperties.ZENDESK_TICKET_ROOT_NODE;

public final class ZendeskRecordPathUtils {

    private static final String NULL_VALUE = "null";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Pattern RECORD_PATH_PATTERN = Pattern.compile("@\\{(/.*?)\\}");

    private ZendeskRecordPathUtils() {
    }

    /**
     * Resolves the input field value to be handled as a record path or a constant value.
     *
     * @param path           path in the request object
     * @param value          field value to be resolved
     * @param baseTicketNode base request object where the field value will be added
     * @param record         record to receive the value from if the field value is a record path
     */
    public static void resolvePropertyValue(String path, String value, ObjectNode baseTicketNode, Record record) {
        final Matcher matcher = RECORD_PATH_PATTERN.matcher(value);
        if (matcher.matches()) {
            addNewNodeAtPath(baseTicketNode, JsonPointer.compile(path), new TextNode(resolveRecordState(matcher.group(1), record)));
        } else {
            addNewNodeAtPath(baseTicketNode, JsonPointer.compile(path), new TextNode(value));
        }
    }

    /**
     * Adds a user defined dynamic field to the request object. If the user specifies the path in the request object as full path (starting with '/ticket' or '/tickets')
     * the method removes them since the root path is specified later based on the number of records.
     *
     * @param path           path in the request object
     * @param value          dynamic field value
     * @param baseTicketNode base request object where the field value will be added
     * @param record         record to receive the value from if the field value is a record path
     */
    public static void addDynamicField(String path, String value, ObjectNode baseTicketNode, Record record) {
        if (path.startsWith(ZENDESK_TICKET_ROOT_NODE)) {
            path = path.substring(7);
        } else if (path.startsWith(ZENDESK_TICKETS_ROOT_NODE)) {
            path = path.substring(8);
        }

        resolvePropertyValue(path, value, baseTicketNode, record);
    }

    private static String resolveRecordState(String pathValue, final Record record) {
        final RecordPath recordPath = RecordPath.compile(pathValue);
        final RecordPathResult result = recordPath.evaluate(record);
        final List<FieldValue> fieldValues = result.getSelectedFields().collect(toList());
        final FieldValue fieldValue = getMatchingFieldValue(recordPath, fieldValues);

        if (fieldValue.getValue() == null || fieldValue.getValue() == NULL_VALUE) {
            return null;
        }

        return getFieldValue(recordPath, fieldValue);
    }

    /**
     * The method checks the field's type and filters out every non-compatible type.
     *
     * @param recordPath path to the requested field
     * @param fieldValue record field
     * @return value of the record field
     */
    private static String getFieldValue(final RecordPath recordPath, FieldValue fieldValue) {
        final RecordFieldType fieldType = fieldValue.getField().getDataType().getFieldType();

        if (fieldType == RecordFieldType.RECORD || fieldType == RecordFieldType.ARRAY || fieldType == RecordFieldType.MAP) {
            throw new ProcessException(String.format("The provided RecordPath [%s] points to a [%s] type value", recordPath, fieldType));
        }

        if (fieldType == RecordFieldType.CHOICE) {
            final ChoiceDataType choiceDataType = (ChoiceDataType) fieldValue.getField().getDataType();
            final List<DataType> possibleTypes = choiceDataType.getPossibleSubTypes();
            if (possibleTypes.stream().anyMatch(type -> type.getFieldType() == RecordFieldType.RECORD)) {
                throw new ProcessException(String.format("The provided RecordPath [%s] points to a [CHOICE] type value with Record subtype", recordPath));
            }
        }

        return String.valueOf(fieldValue.getValue());
    }

    /**
     * The method checks if only one result were received for the give record path.
     *
     * @param recordPath path to the requested field
     * @param resultList result list
     * @return matching field
     */
    private static FieldValue getMatchingFieldValue(final RecordPath recordPath, final List<FieldValue> resultList) {
        if (resultList.isEmpty()) {
            throw new ProcessException(String.format("Evaluated RecordPath [%s] against Record but got no results", recordPath));
        }

        if (resultList.size() > 1) {
            throw new ProcessException(String.format("Evaluated RecordPath [%s] against Record and received multiple distinct results [%s]", recordPath, resultList));
        }

        return resultList.get(0);
    }

    /**
     * Adds a new node on the provided path with the give value to the request object.
     *
     * @param baseNode base object where the new node will be added
     * @param path     path of the new node
     * @param value    value of the new node
     */
    public static void addNewNodeAtPath(final ObjectNode baseNode, final JsonPointer path, final JsonNode value) {
        final JsonPointer parentPointer = path.head();
        final String fieldName = path.last().toString().substring(1);
        JsonNode parentNode = getOrCreateParentNode(baseNode, parentPointer, fieldName);

        setNodeValue(value, fieldName, parentNode);
    }

    private static void setNodeValue(JsonNode value, String fieldName, JsonNode parentNode) {
        if (parentNode.isArray()) {
            ArrayNode arrayNode = (ArrayNode) parentNode;
            int index = Integer.parseInt(fieldName);
            for (int i = arrayNode.size(); i <= index; i++) {
                arrayNode.addNull();
            }
            arrayNode.set(index, value);
        } else if (parentNode.isObject()) {
            ((ObjectNode) parentNode).set(fieldName, value);
        } else {
            throw new IllegalArgumentException("Unsupported node type" + parentNode.getNodeType().name());
        }
    }

    private static JsonNode getOrCreateParentNode(ObjectNode rootNode, JsonPointer parentPointer, String fieldName) {
        JsonNode parentNode = rootNode.at(parentPointer);

        if (parentNode.isMissingNode() || parentNode.isNull()) {
            parentNode = StringUtils.isNumeric(fieldName) ? OBJECT_MAPPER.createArrayNode() : OBJECT_MAPPER.createObjectNode();
            addNewNodeAtPath(rootNode, parentPointer, parentNode);
        }
        return parentNode;
    }
}
