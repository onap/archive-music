package org.onap.music.exceptions;

import org.codehaus.jackson.map.exc.UnrecognizedPropertyException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.modules.junit4.PowerMockRunner;

import javax.ws.rs.core.Response;
import java.io.EOFException;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
public class MusicExceptionMapperTest {

    @Test
    public void testToResponse() {
        MusicExceptionMapper musicExceptionMapper = new MusicExceptionMapper();
        UnrecognizedPropertyException unrecognizedPropertyException = PowerMockito.mock(UnrecognizedPropertyException.class);
        Response response = musicExceptionMapper.toResponse(unrecognizedPropertyException);
        assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertTrue(((Map)response.getEntity()).get("error").toString().startsWith("Unknown field :"));

        EOFException eofException = PowerMockito.mock(EOFException.class);
        response = musicExceptionMapper.toResponse(eofException);
        assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertTrue(((Map)response.getEntity()).get("error").toString().equals("Request body cannot be empty".trim()));

        IllegalArgumentException illegalArgumentException = PowerMockito.mock(IllegalArgumentException.class);
        PowerMockito.when(illegalArgumentException.getMessage()).thenReturn("ERROR MSG");
        response = musicExceptionMapper.toResponse(illegalArgumentException);
        assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertTrue(((Map)response.getEntity()).get("error").toString().equals("ERROR MSG".trim()));
    }
}