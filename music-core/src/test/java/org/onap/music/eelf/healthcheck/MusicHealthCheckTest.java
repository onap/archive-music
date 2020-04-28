/*-
 * ============LICENSE_START============================================
 * ONAP MUSIC
 * =====================================================================
 * Copyright (C) 2020 IBM Intellectual Property. All rights reserved.
 * =====================================================================
 *
 * Unless otherwise specified, all software contained herein is licensed
 * under the Apache License, Version 2.0 (the "License");
 * you may not use this software except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *             http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Unless otherwise specified, all documentation contained herein is licensed
 * under the Creative Commons License, Attribution 4.0 Intl. (the "License");
 * you may not use this documentation except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *             https://creativecommons.org/licenses/by/4.0/
 *
 * Unless required by applicable law or agreed to in writing, documentation
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * ============LICENSE_END================================================
 *
 *
 */
package org.onap.music.eelf.healthcheck;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.main.ResultType;

public class MusicHealthCheckTest {
    MusicHealthCheck musicHealthCheck=new MusicHealthCheck();
    @Test
    public void testCassandraHost()
    {
        musicHealthCheck.setCassandrHost("9042");
        assertEquals("9042", musicHealthCheck.getCassandrHost());
    }

    @Test
    public void testGetCassandraStatus throws MusicServiceException, MusicQueryException()
    {
        MusicHealthCheck mHealthCheck = Mockito.spy(MusicHealthCheck.class);
        doReturn(ResultType.SUCCESS).when(mHealthCheck).nonKeyRelatedPut(Mockito.any(), Mockito.anyString());
        doNothing().when(mHealthCheck).executeEventualPut(Mockito.any());
        assertEquals("ACTIVE", mHealthCheck.getCassandraStatus("consistency"));

    }
}
