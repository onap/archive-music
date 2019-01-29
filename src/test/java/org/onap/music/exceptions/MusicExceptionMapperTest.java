/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 * Copyright (c) 2019 IBM Intellectual Property
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

package org.onap.music.exceptions;

import org.codehaus.jackson.map.exc.UnrecognizedPropertyException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.ws.rs.core.Response;
import java.io.EOFException;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(SpringJUnit4ClassRunner.class)
public class MusicExceptionMapperTest {

    @Test
    public void testToResponse() {
        MusicExceptionMapper musicExceptionMapper = new MusicExceptionMapper();
        UnrecognizedPropertyException unrecognizedPropertyException = mock(UnrecognizedPropertyException.class);
        Response response = musicExceptionMapper.toResponse(unrecognizedPropertyException);
        assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertTrue(((Map)response.getEntity()).get("error").toString().startsWith("Unknown field :"));

        EOFException eofException = mock(EOFException.class);
        response = musicExceptionMapper.toResponse(eofException);
        assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertTrue(((Map)response.getEntity()).get("error").toString().equals("Request body cannot be empty".trim()));

        IllegalArgumentException illegalArgumentException = mock(IllegalArgumentException.class);
        Mockito.when(illegalArgumentException.getMessage()).thenReturn("ERROR MSG");
        response = musicExceptionMapper.toResponse(illegalArgumentException);
        assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertTrue(((Map)response.getEntity()).get("error").toString().equals("ERROR MSG".trim()));
    }
}