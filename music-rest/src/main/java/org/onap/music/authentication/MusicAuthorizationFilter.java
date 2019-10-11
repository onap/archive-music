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

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;

import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.exceptions.MusicAuthenticationException;
import org.onap.music.main.MusicUtil;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * This filter class does authorization from AAF
 *  
 * @author sp931a
 *
 */
//@PropertySource(value = {"file:/opt/app/music/etc/music.properties"})
public class MusicAuthorizationFilter implements Filter {

    private String musicNS = MusicUtil.getMusicAafNs();
    
    private EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(MusicAuthorizationFilter.class);

    public MusicAuthorizationFilter() throws ServletException {
        super();
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        // Do Nothing
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain)
        throws IOException, ServletException {
        HttpServletResponse httpResponse = null;

        boolean isAuthAllowed = false;

        if (null != servletRequest && null != servletResponse) {
            httpResponse = (HttpServletResponse) servletResponse;
            long startTime = 0;
            if( null != servletRequest.getAttribute("startTime")) {
                startTime = ((Long)servletRequest.getAttribute("startTime")).longValue();
            } else {
                startTime = System.currentTimeMillis(); // this will set only incase the request attribute not found
            }

            try {
                isAuthAllowed = AuthUtil.isAccessAllowed(servletRequest, musicNS);
            } catch (MusicAuthenticationException  e) {
                logger.error(EELFLoggerDelegate.securityLogger,
                    "Error while checking authorization Music Namespace: " + musicNS + " : " + e.getMessage(),e);
           } catch ( Exception e) {
                logger.error(EELFLoggerDelegate.securityLogger,
                    "Error while checking authorization Music Namespace: " + musicNS + " : " + e.getMessage(),e);
            }

            long endTime = System.currentTimeMillis();
            
            //startTime set in <code>CadiAuthFilter</code> doFilter
            logger.debug(EELFLoggerDelegate.securityLogger,
                "Time took for authentication & authorization : " 
                + (endTime - startTime) + " milliseconds");

            if (!isAuthAllowed) {
                logger.info(EELFLoggerDelegate.securityLogger,
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
    }

    private byte[] restResponseBytes(AuthorizationError eErrorResponse) throws IOException {
        String serialized = new ObjectMapper().writeValueAsString(eErrorResponse);
        return serialized.getBytes();
    }
}

