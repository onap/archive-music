
package org.onap.music.eelf.healthcheck;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.main.MusicCore;
import org.onap.music.main.ResultType;

public class MusicHealthCheckTest {

    MusicHealthCheck musicHealthCheck;

    @Before
    public void setup() {
        musicHealthCheck = new MusicHealthCheck();
    }

    @Test
    public void testSetCassandrHost() {
        musicHealthCheck.setCassandrHost("127.0.0.1");
        assertEquals("127.0.0.1", musicHealthCheck.getCassandrHost());
    }

    @Test
    public void testGetCassandraStatus() throws MusicServiceException, MusicQueryException {
        MusicHealthCheck mHealthCheck = Mockito.spy(MusicHealthCheck.class);
        doReturn(ResultType.SUCCESS).when(mHealthCheck).nonKeyRelatedPut(Mockito.any(), Mockito.anyString());
        doNothing().when(mHealthCheck).executeEventualPut(Mockito.any());
        assertNotNull(mHealthCheck.getCassandraStatus("consistency"));
//        assertEquals("ACTIVE", mHealthCheck.getCassandraStatus("consistency"));
    }
}

