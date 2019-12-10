/*******************************************************************************
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2018 AT&T Intellectual Property
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
package org.onap.music.service.impl;

import static org.junit.Assert.*;
import java.util.ArrayList;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.exceptions.MusicLockingException;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.lockingservice.cassandra.CassaLockStore;
import org.onap.music.lockingservice.cassandra.CassaLockStore.LockObject;
import org.onap.music.lockingservice.cassandra.LockType;
import org.onap.music.main.MusicUtil;
import org.onap.music.main.ResultType;
import org.onap.music.main.ReturnType;

@RunWith(MockitoJUnitRunner.class)
public class MusicCassaCoreTest {
    
    @Mock
    private CassaLockStore mLockHandle;
    
    MusicCassaCore core;
    
    @Before
    public void before() {
        core = MusicCassaCore.getInstance();
        MusicCassaCore.setmLockHandle(mLockHandle);
    }
    
    @Test
    public void testGetmLockHandle() {
        assertEquals(mLockHandle, MusicCassaCore.getmLockHandle());
    }

    @Test
    public void testSetmLockHandle() {
        CassaLockStore m2 = Mockito.mock(CassaLockStore.class);
        MusicCassaCore.setmLockHandle(m2);
        assertEquals(m2, MusicCassaCore.getmLockHandle());
        //revert back to original handle
        MusicCassaCore.setmLockHandle(mLockHandle);
    }

    @Test
    public void testGetInstance() {
        assertEquals(core, MusicCassaCore.getInstance());
    }

    @Test
    public void testGetLockingServiceHandle() {
        assertEquals(mLockHandle, MusicCassaCore.getmLockHandle());
    }

    @Test
    public void testCreateLockReferenceAtomicString() throws Exception {
        String fullyQualifiedKey = "keyspace.table.lockName";
        Mockito.when(mLockHandle.genLockRefandEnQueue("keyspace", "table", "lockName", LockType.WRITE, null))
                .thenReturn("lockReturned");

        String lockRef = core.createLockReferenceAtomic(fullyQualifiedKey);

        Mockito.verify(mLockHandle).genLockRefandEnQueue("keyspace", "table", "lockName", LockType.WRITE, null);
        assertEquals("lockReturned", lockRef);
    }

    @Test
    public void testCreateLockReferenceStringString() throws Exception {
        String fullyQualifiedKey = "keyspace.table.lockName";
        String owner = "owner1";
        Mockito.when(mLockHandle.genLockRefandEnQueue("keyspace", "table", "lockName", LockType.WRITE, owner))
            .thenReturn("lockReturned");
        
        String lockRef = core.createLockReference(fullyQualifiedKey, owner);

        Mockito.verify(mLockHandle).genLockRefandEnQueue("keyspace", "table", "lockName", LockType.WRITE, owner);
        assertEquals("lockReturned", lockRef);
    }

    @Test
    public void testCreateLockReferenceAtomicStringLockType() throws Exception {
        String fullyQualifiedKey = "keyspace.table.lockName";
        Mockito.when(mLockHandle.genLockRefandEnQueue("keyspace", "table", "lockName", LockType.READ, null))
                .thenReturn("lockReturned");

        String lockRef = core.createLockReferenceAtomic(fullyQualifiedKey, LockType.READ);

        Mockito.verify(mLockHandle).genLockRefandEnQueue("keyspace", "table", "lockName", LockType.READ, null);
        assertEquals("lockReturned", lockRef);
    }

    @Test
    public void testCreateLockReferenceStringLockTypeString() throws Exception {
        String fullyQualifiedKey = "keyspace.table.lockName";
        String owner = "owner1";
        Mockito.when(mLockHandle.genLockRefandEnQueue("keyspace", "table", "lockName", LockType.READ, owner))
            .thenReturn("lockReturned");
        
        String lockRef = core.createLockReference(fullyQualifiedKey, LockType.READ, owner);

        Mockito.verify(mLockHandle).genLockRefandEnQueue("keyspace", "table", "lockName", LockType.READ, owner);
        assertEquals("lockReturned", lockRef);
    }

    @Test
    public void testPromoteLock() throws Exception {
        String lockId = "$keyspace.table.lockName$1";
        Mockito.when(mLockHandle.promoteLock("keyspace", "table", "lockName", "1"))
            .thenReturn(new ReturnType(ResultType.SUCCESS, "Lock Promoted"));
        
        ReturnType rt = core.promoteLock(lockId);
        assertEquals(ResultType.SUCCESS, rt.getResult());
        assertEquals("Lock Promoted", rt.getMessage());
    }

    @Test
    public void testAcquireLockWithLease() throws Exception {
        String fullyQualifiedKey = "keyspace.table.lockName";
        String lockId = "$keyspace.table.lockName$1";
        long leasePeriod = 1000;
        String currTime = String.valueOf(System.currentTimeMillis());
        Mockito.when(mLockHandle.peekLockQueue("keyspace", "table", "lockName"))
            .thenReturn(mLockHandle.new LockObject(true, lockId, currTime, currTime, LockType.WRITE, null));
        Mockito.when(mLockHandle.getLockInfo("keyspace", "table", "lockName", "1"))
            .thenReturn(mLockHandle.new LockObject(false, lockId, null, null, LockType.WRITE, null));
        
        ReturnType rt = core.acquireLockWithLease(fullyQualifiedKey, lockId, leasePeriod);
        assertEquals(ResultType.FAILURE, rt.getResult());
    }

    @Test
    public void testAcquireLock() throws Exception {
        String fullyQualifiedKey = "keyspace.table.lockName";
        String lockId = "$keyspace.table.lockName$1";
        Mockito.when(mLockHandle.getLockInfo("keyspace", "table", "lockName", "1"))
            .thenReturn(mLockHandle.new LockObject(false, lockId, null, null, LockType.WRITE, null));
        
        ReturnType rt = core.acquireLock(fullyQualifiedKey, lockId);
        
        assertEquals(ResultType.FAILURE, rt.getResult());
        Mockito.verify(mLockHandle).getLockInfo("keyspace", "table", "lockName", "1");
        /*TODO: if we successfully acquire the lock we hit an error by trying to read MusicDatastoreHandle */
    }

    @Test
    public void testWhoseTurnIsIt() throws Exception {
        String fullyQualifiedKey = "keyspace.table.lockName";
        Mockito.when(mLockHandle.peekLockQueue("keyspace", "table", "lockName"))
            .thenReturn(mLockHandle.new LockObject(true, "1", "", "", LockType.WRITE, null));
        
        String topOfQ = core.whoseTurnIsIt(fullyQualifiedKey);
        System.out.println(topOfQ);
        
        assertEquals("$"+fullyQualifiedKey+"$1", topOfQ);
    }

    @Test
    public void testGetCurrentLockHolders() throws Exception {
        String fullyQualifiedKey = "keyspace.table.lockName";
        List<String> currentHolders = new ArrayList<>();
        currentHolders.add("$"+fullyQualifiedKey+"$1");
        currentHolders.add("$"+fullyQualifiedKey+"$2");
        Mockito.when(mLockHandle.getCurrentLockHolders("keyspace", "table", "lockName"))
            .thenReturn(currentHolders);

        List<String> holders = core.getCurrentLockHolders(fullyQualifiedKey);
        
        assertTrue(currentHolders.containsAll(holders) && holders.containsAll(currentHolders));
    }

    @Test
    public void testGetLockNameFromId() {
        String lockId = "$keyspace.table.lockName$1";
        assertEquals("keyspace.table.lockName", core.getLockNameFromId(lockId));
    }

    @Test
    public void testDestroyLockRefString() throws Exception {
        String lockId = "$keyspace.table.lockName$1";

        core.destroyLockRef(lockId);
        Mockito.verify(mLockHandle).deQueueLockRef("keyspace", "table", "lockName", "1", MusicUtil.getRetryCount());
    }

    @Test
    public void testDestroyLockRefStringString() throws Exception {
        String fullyQualifiedKey = "keyspace.table.lockName";
        String lockReference = "1";
        
        core.destroyLockRef(fullyQualifiedKey, lockReference);
        Mockito.verify(mLockHandle).deQueueLockRef("keyspace", "table", "lockName", "1", MusicUtil.getRetryCount());
    }

    @Test
    public void testReleaseLock() throws Exception {
        String lockId = "$keyspace.table.lockName$1";
        
        core.releaseLock(lockId, true);
        
        Mockito.verify(mLockHandle).deQueueLockRef("keyspace", "table", "lockName", "1", MusicUtil.getRetryCount());
    }

    @Test
    public void testVoluntaryReleaseLock() throws Exception {
        String fullyQualifiedKey = "keyspace.table.lockName";
        String lockReference = "1";
        
        core.voluntaryReleaseLock(fullyQualifiedKey, lockReference);
        
        Mockito.verify(mLockHandle).deQueueLockRef("keyspace", "table", "lockName", "1", MusicUtil.getRetryCount());
    }

    @Test
    public void testReleaseAllLocksForOwner() throws Exception {
        List<String> ownersLocks = new ArrayList<>();
        ownersLocks.add("lockName$1");
        ownersLocks.add("lockName$2");
        Mockito.when(mLockHandle.getAllLocksForOwner("ownerId", "keyspace", "table"))
            .thenReturn(ownersLocks);
        
        List<String> locksReleased = core.releaseAllLocksForOwner("ownerId", "keyspace", "table");
        
        Mockito.verify(mLockHandle).deQueueLockRef("keyspace", "table", "lockName", "1", MusicUtil.getRetryCount());
        Mockito.verify(mLockHandle).deQueueLockRef("keyspace", "table", "lockName", "2", MusicUtil.getRetryCount());
        assertTrue(ownersLocks.containsAll(locksReleased) && locksReleased.containsAll(ownersLocks));
    }


    @Test
    public void testValidateLock() {
        String lockId = "$keyspace.table.lockName$1";
        
        assertFalse(core.validateLock(lockId).containsKey("Error"));
    }

    @Test
    public void testGetLockQueue() throws Exception {
        String fullyQualifiedKey = "keyspace.table.lockName";
        List<String> myList = new ArrayList<>();
        Mockito.when(mLockHandle.getLockQueue("keyspace", "table", "lockName"))
            .thenReturn(myList);
        List<String> theirList = core.getLockQueue(fullyQualifiedKey);
        
        assertEquals(myList, theirList);
    }

    @Test
    public void testGetLockQueueSize() throws Exception {
        String fullyQualifiedKey = "keyspace.table.lockName";
        Mockito.when(mLockHandle.getLockQueueSize("keyspace", "table", "lockName"))
            .thenReturn((long) 23);
        long theirSize = core.getLockQueueSize(fullyQualifiedKey);
        
        assertEquals(23, theirSize);
    }

}
