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
package org.apache.nifi.commons.zendesk;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processors.zendesk.ZendeskAuthenticationType;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;

import static org.apache.nifi.expression.ExpressionLanguageScope.FLOWFILE_ATTRIBUTES;
import static org.apache.nifi.processor.util.StandardValidators.NON_BLANK_VALIDATOR;
import static org.apache.nifi.processor.util.StandardValidators.NON_EMPTY_VALIDATOR;

public class ZendeskProperties {

    public static final String WEB_CLIENT_SERVICE_PROVIDER_NAME = "web-client-service-provider";
    public static final String ZENDESK_SUBDOMAIN_NAME = "zendesk-subdomain";
    public static final String ZENDESK_USER_NAME = "zendesk-user";
    public static final String ZENDESK_AUTHENTICATION_TYPE_NAME = "zendesk-authentication-type-name";
    public static final String ZENDESK_AUTHENTICATION_CREDENTIAL_NAME = "zendesk-authentication-value-name";
    public static final String ZENDESK_TICKET_COMMENT_BODY_NAME = "zendesk-comment-body";
    public static final String ZENDESK_TICKET_SUBJECT_NAME = "zendesk-subject";
    public static final String ZENDESK_TICKET_PRIORITY_NAME = "zendesk-priority";
    public static final String ZENDESK_TICKET_TYPE_NAME = "zendesk-type";
    public static final String HTTPS = "https";
    public static final String AUTHORIZATION_HEADER_NAME = "Authorization";
    public static final String CONTENT_TYPE_HEADER_NAME = "Content-Type";
    public static final String CONTENT_TYPE_APPLICATION_JSON = "application/json";
    public static final String BASIC_AUTH_PREFIX = "Basic ";
    public static final String ZENDESK_HOST_TEMPLATE = "%s.zendesk.com";

    public static final String ZENDESK_CREATE_TICKET_RESOURCE = "/api/v2/tickets";
    public static final String ZENDESK_CREATE_TICKETS_RESOURCE = "/api/v2/tickets/create_many";

    public static final String ZENDESK_TICKET_ROOT_NODE = "/ticket";
    public static final String ZENDESK_TICKETS_ROOT_NODE = "/tickets";

    public static final String REL_SUCCESS_NAME = "success";
    public static final String REL_FAILURE_NAME = "failure";

    public static final PropertyDescriptor WEB_CLIENT_SERVICE_PROVIDER = new PropertyDescriptor.Builder()
            .name(WEB_CLIENT_SERVICE_PROVIDER_NAME)
            .displayName("Web Client Service Provider")
            .description("Controller service for HTTP client operations.")
            .identifiesControllerService(WebClientServiceProvider.class)
            .required(true)
            .build();

    public static final PropertyDescriptor ZENDESK_SUBDOMAIN = new PropertyDescriptor.Builder()
            .name(ZENDESK_SUBDOMAIN_NAME)
            .displayName("Zendesk Subdomain Name")
            .description("Name of the Zendesk subdomain.")
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .required(true)
            .addValidator(NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor ZENDESK_USER = new PropertyDescriptor.Builder()
            .name(ZENDESK_USER_NAME)
            .displayName("Zendesk User Name")
            .description("Login user to Zendesk subdomain.")
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .required(true)
            .addValidator(NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor ZENDESK_AUTHENTICATION_TYPE = new PropertyDescriptor.Builder()
            .name(ZENDESK_AUTHENTICATION_TYPE_NAME)
            .displayName("Zendesk Authentication Type")
            .description("Type of authentication to Zendesk API.")
            .required(true)
            .allowableValues(ZendeskAuthenticationType.class)
            .build();

    public static final PropertyDescriptor ZENDESK_AUTHENTICATION_CREDENTIAL = new PropertyDescriptor.Builder()
            .name(ZENDESK_AUTHENTICATION_CREDENTIAL_NAME)
            .displayName("Zendesk Authentication Credential")
            .description("Password or authentication token for Zendesk login user.")
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .sensitive(true)
            .required(true)
            .addValidator(NON_EMPTY_VALIDATOR)
            .build();
}
