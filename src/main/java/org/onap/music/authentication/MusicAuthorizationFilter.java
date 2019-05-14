/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2017 AT&T Intellectual Property
 * ===================================================================
 *  Modifications Copyright (c) 2019 Samsung
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

package org.onap.music.authentication;

import java.io.IOException;
import java.util.Base64;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * This filter class does authorization from AAF
 *  
 * @author sp931a
 *
 */
@PropertySource(value = {"file:/opt/app/music/etc/music.properties"})
public class MusicAuthorizationFilter implements Filter {

    @Value("${music.aaf.ns}")
    private String musicNS;
    
    private EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(MusicAuthorizationFilter.class);

    public MusicAuthorizationFilter() throws ServletException {
        super();
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {

    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain)
            throws IOException, ServletException {

        logger.debug(EELFLoggerDelegate.applicationLogger,
                "In MusicAuthorizationFilter doFilter start() ::::::::::::::::::::::::");

        HttpServletResponse httpResponse = null;

        boolean isAuthAllowed = false;

        if (null != servletRequest && null != servletResponse) {
            httpResponse = (HttpServletResponse) servletResponse;

            logger.debug(EELFLoggerDelegate.applicationLogger,
                    "Music NS defined in music property file  --------------------------" + musicNS);
            
            long startTime = 0;
            if( null != servletRequest.getAttribute("startTime")) {
                startTime = ((Long)servletRequest.getAttribute("startTime")).longValue();
            } else {
                startTime = System.currentTimeMillis(); // this will set only incase the request attribute not found
            }

            try {
                isAuthAllowed = AuthUtil.isAccessAllowed(servletRequest, musicNS);
            } catch (Exception e) {
                logger.error(EELFLoggerDelegate.applicationLogger,
                        "Error while checking authorization :::" + e.getMessage());
            }

            long endTime = System.currentTimeMillis();
            
            //startTime set in <code>CadiAuthFilter</code> doFilter
            logger.debug(EELFLoggerDelegate.applicationLogger,
                    "Time took for authentication & authorization : " 
                    + (endTime - startTime) + " milliseconds");

            if (!isAuthAllowed) {
                logger.debug(EELFLoggerDelegate.applicationLogger,
                    "Unauthorized Access");
                AuthorizationError authError = new AuthorizationError();
                authError.setResponseCode(HttpServletResponse.SC_UNAUTHORIZED);
                authError.setResponseMessage("Unauthorized Access - Please make sure you are "
                    + "onboarded and have proper access to MUSIC. ");

                byte[] responseToSend = restResponseBytes(authError);
                httpResponse.setHeader("Content-Type", "application/json");

                httpResponse.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                servletResponse.getOutputStream().write(responseToSend);
                return;
            } else {
                filterChain.doFilter(servletRequest, servletResponse);
            }
        }
        logger.debug(EELFLoggerDelegate.applicationLogger,
                "In MusicAuthorizationFilter doFilter exit() ::::::::::::::::::::::::");
    }

    private byte[] restResponseBytes(AuthorizationError eErrorResponse) throws IOException {
        String serialized = new ObjectMapper().writeValueAsString(eErrorResponse);
        return serialized.getBytes();
    }

    private Map<String, String> getHeadersInfo(HttpServletRequest request) {

        Map<String, String> map = new HashMap<String, String>();

        Enumeration headerNames = request.getHeaderNames();
        while (headerNames.hasMoreElements()) {
            String key = (String) headerNames.nextElement();
            String value = request.getHeader(key);
            map.put(key, value);
        }

        return map;
    }

    private static String getUserNamefromRequest(HttpServletRequest httpRequest) {
        String authHeader = httpRequest.getHeader("Authorization");
        String username = null;
        if (authHeader != null) {
            String[] split = authHeader.split("\\s+");
            if (split.length > 0) {
                String basic = split[0];

                if ("Basic".equalsIgnoreCase(basic)) {
                    byte[] decodedBytes = Base64.getDecoder().decode(split[1]);
                    String decodedString = new String(decodedBytes);
                    int p = decodedString.indexOf(":");
                    if (p != -1) {
                        username = decodedString.substring(0, p);
                    }
                }
            }
        }
        return username;
    }
}
