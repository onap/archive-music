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

package org.onap.music.lockingservice.cassandra;

import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;
import org.onap.music.lockingservice.cassandra.MusicLockState.LockStatus;

public class MusicLockStateTest {
    
    MusicLockState musicLockState;
    
    @Before
    public void setup() {
        musicLockState = new MusicLockState(LockStatus.LOCKED, "", true);
    }
    
    @Test
    public void testGetLeasePeriod() {
        musicLockState.setLeasePeriod(200L);
        assertEquals(200L, musicLockState.getLeasePeriod());
    }
    
    @Test
    public void testIsNeedToSyncQuorum() {
        assertEquals(true, musicLockState.isNeedToSyncQuorum());
    }
    
    @Test
    public void testGetLeaseStartTime() {
        musicLockState.setLeaseStartTime(200L);
        assertEquals(200L, musicLockState.getLeaseStartTime());
    }
    
    @Test
    public void testGetLockStatus() {
        musicLockState.setLockStatus(LockStatus.LOCKED);
        assertEquals(LockStatus.LOCKED, musicLockState.getLockStatus());
    }
    
    @Test
    public void testGetLockHolder() {
        musicLockState.setLockHolder("lockHolder");
        assertEquals("lockHolder", musicLockState.getLockHolder());
    }
    
    @Test
    public void testGetErrorMessage() {
        MusicLockState musicLockState2 = new MusicLockState("This is error message");
        assertEquals("This is error message", musicLockState2.getErrorMessage());
    }
    
    @Test
    public void testSerialize() {
        byte[] serializedBytes = musicLockState.serialize();
        MusicLockState musicLockState3 = musicLockState.deSerialize(serializedBytes);
        assertEquals(musicLockState.getLeasePeriod(),musicLockState3.getLeasePeriod());
        assertEquals(musicLockState.isNeedToSyncQuorum(),musicLockState3.isNeedToSyncQuorum());
        assertEquals(musicLockState.getLeaseStartTime(),musicLockState3.getLeaseStartTime());
        assertEquals(musicLockState.getLockStatus(),musicLockState3.getLockStatus());
        assertEquals(musicLockState.getLockHolder(),musicLockState3.getLockHolder());
        assertEquals(musicLockState.getErrorMessage(),musicLockState3.getErrorMessage());
    }
    
}
