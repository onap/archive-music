/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2017 AT&T Intellectual Property
 * ===================================================================
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * 
 * ============LICENSE_END=============================================
 * ====================================================================
 */
package org.onap.music.eelf.logging;

import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.onap.music.authentication.AuthorizationError;
import org.onap.music.main.MusicUtil;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 
 * This is the first filter in the chain to be executed before cadi
 * authentication. The priority has been set in <code>MusicApplication</code>
 * through filter registration bean
 * 
 * The responsibility of this filter is to validate header values as per
 * contract and write it to MDC and http response header back.
 * 
 * 
 * @author sp931a
 *
 */

public class MusicLoggingServletFilter implements Filter {

    private EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(MusicLoggingServletFilter.class);
    // client transaction id, specific to client system, set in properties
    public static final String CONVERSATION_ID = MusicUtil.getConversationIdPrefix() + "ConversationId";

    // can be used as correlation-id in case of callback, also this can be passed to
    // other services for tracking.
    public static final String MESSAGE_ID = MusicUtil.getMessageIdPrefix() + "MessageId";

    // client id would be the unique client source-system-id, i;e VALET or CONDUCTOR
    // etc
    public static final String CLIENT_ID = MusicUtil.getClientIdPrefix() + "ClientId";

    // unique transaction of the source system
    private static final String TRANSACTION_ID = MusicUtil.getTransIdPrefix() + "Transaction-Id";

    public MusicLoggingServletFilter() throws ServletException {
        super();
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {

    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {

        logger.info(EELFLoggerDelegate.applicationLogger,
                "In MusicLogginServletFilter doFilter start() ::::::::::::::::::::::: [\"+MusicUtil.getTransIdRequired()+\",\"+MusicUtil.getConversationIdRequired()+\",\"+MusicUtil.getClientIdRequired()+\",\"+MusicUtil.getMessageIdRequired()");

        HttpServletRequest httpRequest = null;
        HttpServletResponse httpResponse = null;
        Map<String, String> headerMap = null;
        Map<String, String> upperCaseHeaderMap = null;

        if (null != request && null != response) {
            httpRequest = (HttpServletRequest) request;
            httpResponse = (HttpServletResponse) response;

            headerMap = getHeadersInfo(httpRequest);

            // The custom header values automatically converted into lower case, not sure
            // why ? So i had to covert all keys to upper case
            // The response header back to client will have all custom header values as
            // upper case.
            upperCaseHeaderMap = headerMap.entrySet().stream()
                    .collect(Collectors.toMap(entry -> entry.getKey().toUpperCase(), entry -> entry.getValue()));
            // Enable/disable keys are present in /opt/app/music/etc/music.properties

            if (Boolean.valueOf(MusicUtil.getTransIdRequired())
                    && !upperCaseHeaderMap.containsKey(TRANSACTION_ID.toUpperCase())) {
                populateError(httpResponse, "Transaction id '" + TRANSACTION_ID 
                    + "' required on http header");
                return;
            } else {
                populateMDCAndResponseHeader(upperCaseHeaderMap, TRANSACTION_ID, "transactionId",
                    Boolean.valueOf(MusicUtil.getTransIdRequired()), httpResponse);
            }

            if (Boolean.valueOf(MusicUtil.getConversationIdRequired())
                && !upperCaseHeaderMap.containsKey(CONVERSATION_ID.toUpperCase())) {
                populateError(httpResponse, "Conversation Id '" + CONVERSATION_ID 
                    + "' required on http header");
                return;
            } else {
                populateMDCAndResponseHeader(upperCaseHeaderMap, CONVERSATION_ID, "conversationId",
                    Boolean.valueOf(MusicUtil.getConversationIdRequired()), httpResponse);
            }

            if (Boolean.valueOf(MusicUtil.getMessageIdRequired())
                && !upperCaseHeaderMap.containsKey(MESSAGE_ID.toUpperCase())) {
                populateError(httpResponse, "Message Id '" + MESSAGE_ID 
                    + "' required on http header");
                return;
            } else {
                populateMDCAndResponseHeader(upperCaseHeaderMap, MESSAGE_ID, "messageId",
                    Boolean.valueOf(MusicUtil.getMessageIdRequired()), httpResponse);
            }

            if (Boolean.valueOf(MusicUtil.getClientIdRequired())
                && !upperCaseHeaderMap.containsKey(CLIENT_ID.toUpperCase())) {
                populateError(httpResponse, "Client Id '" + CLIENT_ID 
                    + "' required on http header");
                return;
            } else {
                populateMDCAndResponseHeader(upperCaseHeaderMap, CLIENT_ID, "clientId",
                    Boolean.valueOf(MusicUtil.getClientIdRequired()), httpResponse);
            }

        }

        logger.info(EELFLoggerDelegate.applicationLogger,
                "In MusicLogginServletFilter doFilter. Header values validated sucessfully");

        chain.doFilter(request, response);
    }

    private void populateError(HttpServletResponse httpResponse, String errMsg) throws IOException {
        AuthorizationError authError = new AuthorizationError();
        authError.setResponseCode(HttpServletResponse.SC_BAD_REQUEST);
        authError.setResponseMessage(errMsg);

        byte[] responseToSend = restResponseBytes(authError);
        httpResponse.setHeader("Content-Type", "application/json");

        // ideally the http response code should be 200, as this is a biz validation
        // failure. For now, keeping it consistent with other places.
        httpResponse.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        httpResponse.getOutputStream().write(responseToSend);
    }

    private void populateMDCAndResponseHeader(Map<String, String> headerMap, String idKey, String mdcKey,
            boolean isRequired, HttpServletResponse httpResponse) throws ServletException, IOException {

        idKey = idKey.trim().toUpperCase();

        // 1. setting the keys & value in MDC for future use 2.setting the values in
        // http response header back to client.
        if (isRequired && (headerMap.containsKey(idKey))) {
            EELFLoggerDelegate.mdcPut(mdcKey, headerMap.get(idKey));
            httpResponse.addHeader(idKey, headerMap.get(idKey));
        } else {
            // do nothing
        }
    }

    private Map<String, String> getHeadersInfo(HttpServletRequest request) {

        Map<String, String> map = new HashMap<String, String>();

        Enumeration<String> headerNames = request.getHeaderNames();
        while (headerNames.hasMoreElements()) {
            String key = (String) headerNames.nextElement();
            String value = request.getHeader(key);
            map.put(key, value);
        }

        return map;
    }

    private byte[] restResponseBytes(AuthorizationError eErrorResponse) throws IOException {
        String serialized = new ObjectMapper().writeValueAsString(eErrorResponse);
        return serialized.getBytes();
    }
}
