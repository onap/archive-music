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

package org.onap.music.datastore.jsonobjects;

import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;
import org.onap.music.lockingservice.cassandra.LockType;

public class JsonLockTest {

    JsonLock jsonLock;
    
    @Before
    public void setup() {
        jsonLock = new JsonLock();
    }
    
    @Test
    public void testSetLockType() {
        jsonLock.setLockType(LockType.READ);
        assertEquals(LockType.READ, jsonLock.getLocktype());
        
        jsonLock.setLockType(LockType.WRITE);
        assertEquals(LockType.WRITE, jsonLock.getLocktype());
        
        jsonLock.setLockType(LockType.PROMOTING);
        assertEquals(LockType.PROMOTING, jsonLock.getLocktype());
    }
}
