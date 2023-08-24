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
package org.apache.nifi.services.zendesk;

import org.apache.nifi.context.PropertyContext;

import static org.apache.nifi.services.zendesk.ZendeskRecordSink.ZENDESK_TICKET_COMMENT_BODY;
import static org.apache.nifi.services.zendesk.ZendeskRecordSink.ZENDESK_TICKET_PRIORITY;
import static org.apache.nifi.services.zendesk.ZendeskRecordSink.ZENDESK_TICKET_SUBJECT;
import static org.apache.nifi.services.zendesk.ZendeskRecordSink.ZENDESK_TICKET_TYPE;

public class BaseSinkZendeskTicket {

    private final String commentBody;
    private final String subject;
    private final String priority;
    private final String type;

    public BaseSinkZendeskTicket(PropertyContext context) {
        this.commentBody = context.getProperty(ZENDESK_TICKET_COMMENT_BODY).evaluateAttributeExpressions().getValue();
        this.subject = context.getProperty(ZENDESK_TICKET_SUBJECT).evaluateAttributeExpressions().getValue();
        this.priority = context.getProperty(ZENDESK_TICKET_PRIORITY).evaluateAttributeExpressions().getValue();
        this.type = context.getProperty(ZENDESK_TICKET_TYPE).evaluateAttributeExpressions().getValue();
    }

    public String getCommentBody() {
        return commentBody;
    }

    public String getSubject() {
        return subject;
    }

    public String getPriority() {
        return priority;
    }

    public String getType() {
        return type;
    }
}
