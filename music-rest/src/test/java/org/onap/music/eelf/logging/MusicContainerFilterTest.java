/*******************************************************************************
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2019 AT&T Intellectual Property
 * ===================================================================
 * Modifications Copyright (C) 2020 IBM.
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

import java.io.IOException;

import javax.servlet.ServletException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;

import org.springframework.stereotype.Component;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.onap.music.main.MusicUtil;

public class MusicContainerFilterTest {
    MusicContainerFilter filter;

    @Before
    public void setup() throws IOException {
        filter = new MusicContainerFilter();
    }

    @Test
    public void testDoFilter() throws IOException {
        ContainerResponseFilter fil=Mockito.mock(ContainerResponseFilter.class);
        ContainerRequestContext req=Mockito.mock(ContainerRequestContext.class);
        ContainerResponseContext res=Mockito.mock(ContainerResponseContext.class);
        filter.filter(req,res);
    }

}
