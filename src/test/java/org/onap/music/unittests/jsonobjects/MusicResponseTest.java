/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2019 IBM.
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

package org.onap.music.unittests.jsonobjects;


import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;
import org.onap.music.datastore.jsonobjects.MusicResponse;

public class MusicResponseTest {

    private MusicResponse musicResponse;
    
    @Before
    public void setUp()
    {
        musicResponse = new MusicResponse();
    }
    
    @Test
    public void testStatus()
    {
        musicResponse.setStatus("Status");
        assertEquals("Status", musicResponse.getStatus());
    }
    
    @Test
    public void testMessage()
    {
        musicResponse.setMessage("Message");
        assertEquals("Message", musicResponse.getMessage());
    }
    
    @Test
    public void testSucces()
    {
        musicResponse.setSucces(true);
        assertEquals(true, musicResponse.isSucces());
    }
}
