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
import org.onap.music.main.MusicDigest;

public class MusicDigestTest {
    
    private MusicDigest musicDigest;

    @Before
    public void setUp()
    {
        musicDigest= new MusicDigest("evPutStatus", "vectorTs");
    }
    
    @Test
    public void testGetSetEvPutStatus()
    {
        musicDigest.setEvPutStatus("evPutStatus");
        assertEquals("evPutStatus", musicDigest.getEvPutStatus());
    }
    
    @Test
    public void testGetSetVectorTs()
    {
        musicDigest.setVectorTs("vectorTs");
        assertEquals("vectorTs", musicDigest.getVectorTs());
    }
    
    @Test
    public void testToString()
    {
        assertEquals("vectorTs|evPutStatus", musicDigest.toString());
    }
}
