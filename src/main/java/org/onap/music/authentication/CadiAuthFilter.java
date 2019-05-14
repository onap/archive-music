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

package org.onap.music.authentication;


import java.io.IOException;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.annotation.WebFilter;

import org.onap.aaf.cadi.PropAccess;
import org.onap.aaf.cadi.filter.CadiFilter;
import org.onap.music.eelf.logging.EELFLoggerDelegate;

@WebFilter(urlPatterns = { "/*" })
public class CadiAuthFilter extends CadiFilter {

    private static final EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(CadiAuthFilter.class);

    public CadiAuthFilter(PropAccess access) throws ServletException {
        super(true, access);
    }

    public CadiAuthFilter() throws ServletException {
        super();
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        super.init(filterConfig);
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
            logger.info(EELFLoggerDelegate.applicationLogger, "Request is entering cadifilter");
            
            long startTime = System.currentTimeMillis();
            request.setAttribute("startTime", startTime);
            
            super.doFilter(request, response, chain);
            
            //Commented by saumya (sp931a) on 04/11/19 for auth filter
            //chain.doFilter(request, response);
    }
}