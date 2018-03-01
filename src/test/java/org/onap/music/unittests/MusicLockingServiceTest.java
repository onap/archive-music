/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2017 AT&T Intellectual Property
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

package org.onap.music.unittests;

import static org.junit.Assert.*;
import java.io.File;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.onap.music.exceptions.MusicLockingException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.lockingservice.MusicLockState;
import org.onap.music.lockingservice.MusicLockingService;
import org.onap.music.lockingservice.ZkStatelessLockService;
import org.onap.music.lockingservice.MusicLockState.LockStatus;

public class MusicLockingServiceTest {
    
    static MusicLockingService mLockHandle;
    static TestingServer zkServer;
    
    @BeforeClass
    public static void init() throws Exception {
        try {
            zkServer = new TestingServer(2181,new File("/tmp/zk"));

        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("####" + zkServer.getPort());
        try {
            mLockHandle = new MusicLockingService();
        } catch (MusicServiceException e) {
            e.printStackTrace();
        }
        
    
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        zkServer.stop();
        mLockHandle.close();
    }

    @Before
    public void setUp() throws Exception {}

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testMusicLockingService() {
        assertTrue(mLockHandle != null);
    }

    @Test
    public void testGetzkLockHandle() {
        ZkStatelessLockService lockHandle = mLockHandle.getzkLockHandle();
        assertTrue(lockHandle != null);
    }

    @Test
    public void testMusicLockingServiceString() {
       // MusicLockingService mLockTest = new MusicLockingService("localhost");
       // assertTrue(mLockTest != null);
       // mLockTest.close();
    }

    @Test
    public void testCreateLockaIfItDoesNotExist() {
        
        mLockHandle.createLockaIfItDoesNotExist("/ks1.tb1.pk1");
        MusicLockState mls = null;
        try {
           // mls = mLockHandle.
            mls = mLockHandle.getLockState("ks1.tb1.pk1");
        } catch (MusicLockingException e) {
            e.printStackTrace();
        }
        System.out.println("Lock Holder:" + mls.getLockHolder());
        assertFalse(mls.getLeaseStartTime() > 0);
    }
    
    @Test
    public void testSetLockState() {
        MusicLockState musicLockState = new MusicLockState(LockStatus.UNLOCKED, "id1");
        mLockHandle.setLockState("ks1.tb1.pk1",musicLockState);
        MusicLockState mls = null;
        try {
             mls = mLockHandle.getLockState("ks1.tb1.pk1");
        } catch (MusicLockingException e) {
             e.printStackTrace();
        }
        assertEquals(musicLockState.getLockHolder(), mls.getLockHolder());

    }

    @Test
    public void testGetLockState() {
        MusicLockState mls = null;
        try {
             mls = mLockHandle.getLockState("ks1.tb1.pk1");
        } catch (MusicLockingException e) {
             e.printStackTrace();
        }
        assertTrue(mls.getLockHolder().equals("id1"));
    }

//    @Test
//    public void testCreateLockId() {
//        
//        fail("Not yet implemented"); // TODO
//    }
//
//    @Test
//    public void testIsMyTurn() {
//        fail("Not yet implemented"); // TODO
//    }
//
//    @Test
//    public void testUnlockAndDeleteId() {
//        fail("Not yet implemented"); // TODO
//    }
//
//    @Test
//    public void testDeleteLock() {
//        fail("Not yet implemented"); // TODO
//    }
//
//    @Test
//    public void testWhoseTurnIsIt() {
//        fail("Not yet implemented"); // TODO
//    }
//
//    @Test
//    public void testProcess() {
//        fail("Not yet implemented"); // TODO
//    }
//
//    @Test
//    public void testClose() {
//        fail("Not yet implemented"); // TODO
//    }
//
//    @Test
//    public void testLockIdExists() {
//        fail("Not yet implemented"); // TODO
//    }

}
