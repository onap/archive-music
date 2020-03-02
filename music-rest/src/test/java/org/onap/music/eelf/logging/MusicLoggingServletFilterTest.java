/*******************************************************************************
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2019 AT&T Intellectual Property
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
 *******************************************************************************/
package org.onap.music.eelf.logging;

import static org.mockito.Mockito.doNothing;
import java.io.IOException;
import java.util.Enumeration;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.onap.music.main.MusicUtil;

public class MusicLoggingServletFilterTest {
    MusicLoggingServletFilter filter;
    
    @Before
    public void setup() throws ServletException {
        filter = new MusicLoggingServletFilter();
    }
    
    @Test
    public void testDoFilter() throws IOException, ServletException {
        FilterChain chain = Mockito.mock(FilterChain.class);
        Enumeration<String> headerNames = Mockito.mock(Enumeration.class);
        HttpServletRequest httpRequest = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse httpResponse = Mockito.mock(HttpServletResponse.class);
        Mockito.when(headerNames.hasMoreElements()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        Mockito.when(headerNames.nextElement()).thenReturn("element1").thenReturn("element2").thenReturn("element3");
        Mockito.when(httpRequest.getHeader(Mockito.anyString())).thenReturn("key1").thenReturn("key2").thenReturn("key3");
        Mockito.when(httpRequest.getHeaderNames()).thenReturn(headerNames);
        MusicUtil.setTransIdRequired(false);
        MusicUtil.setConversationIdRequired(false);
        MusicUtil.setMessageIdRequired(false);
        MusicUtil.setClientIdRequired(false);
        doNothing().when(chain).doFilter(Mockito.any(), Mockito.any());
        filter.doFilter(httpRequest, httpResponse, chain);
    }
    
}
