/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2020 IBM Intellectual Property
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
        //assertNull(mHealthCheck.getCassandraStatus("consistency"));
        try{
             assertEquals("ACTIVE", mHealthCheck.getCassandraStatus("consistency"));
           }
        catch(Exception e){
              }
    }
}

