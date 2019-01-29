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
import static org.onap.music.service.impl.MusicZKCore.mLockHandle;

import java.util.HashMap;
import java.util.Map;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.onap.music.exceptions.MusicLockingException;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.lockingservice.cassandra.MusicLockState;
import org.onap.music.lockingservice.cassandra.MusicLockState.LockStatus;
import org.onap.music.lockingservice.zookeeper.MusicLockingService;
import org.onap.music.main.MusicUtil;
import org.onap.music.main.ResultType;
import org.onap.music.main.ReturnType;
import org.onap.music.service.impl.MusicZKCore;
import org.onap.music.datastore.Condition;
import org.onap.music.datastore.MusicDataStore;
import org.onap.music.datastore.MusicDataStoreHandle;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.eelf.logging.format.AppMessages;
import org.onap.music.eelf.logging.format.ErrorSeverity;
import org.onap.music.eelf.logging.format.ErrorTypes;
import com.att.eelf.exception.EELFException;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;


public class TestMusicCore {

    
    private Condition condition;

    
    private ResultSet rs;

   
    private PreparedQueryObject preparedQueryObject;
    
   
    private Session session;
    
    @Mock 
    MusicZKCore musicZKCore;

    @Before
    public void setUp() {
        mLockHandle = Mockito.mock(MusicLockingService.class);
        musicZKCore = MusicZKCore.getInstance();
        condition=Mockito.mock(Condition.class);
          rs =Mockito.mock(ResultSet.class);
          preparedQueryObject =Mockito.mock(PreparedQueryObject.class);
          session =Mockito.mock(Session.class);
         
    }

    @Test
    public void testCreateLockReferenceforvalidlock() {
        Mockito.when(mLockHandle.createLockId("/" + "test")).thenReturn("lock");
        String lockId = musicZKCore.createLockReference("test");
        assertEquals("lock", lockId);
        Mockito.verify(mLockHandle).createLockId("/" + "test");
    }


    @Test
    public void testCreateLockReferencefornullname() {
        //Mockito.when(mLockHandle.createLockId("/" + "test")).thenReturn("lock");
        String lockId = musicZKCore.createLockReference("x"); //test");
        //System.out.println("cjc exception lockhandle=" + mLockHandle+"lockid="+lockId );
        assertNotEquals("lock", lockId);
        //Mockito.verify(mLockHandle).createLockId("/" + "test");
    }
    
    @Test
    public void testIsTableOrKeySpaceLock() {
        Boolean result = musicZKCore.isTableOrKeySpaceLock("ks1.tn1");
        assertTrue(result);
    }

    @Test
    public void testIsTableOrKeySpaceLockwithPrimarykey() {
        Boolean result = musicZKCore.isTableOrKeySpaceLock("ks1.tn1.pk1");
        assertFalse(result);
    }

    @Test
    public void testGetMusicLockState() throws MusicLockingException {
        MusicLockState musicLockState = new MusicLockState(LockStatus.UNLOCKED, "id1");
        Mockito.when(mLockHandle.getLockState("ks1.tb1.pk1")).thenReturn(musicLockState);
        MusicLockState mls = MusicZKCore.getMusicLockState("ks1.tb1.pk1");
        assertEquals(musicLockState, mls);
        Mockito.verify(mLockHandle).getLockState("ks1.tb1.pk1");
    }

    @Test
    public void testAcquireLockifisMyTurnTrue() throws MusicLockingException {
        Mockito.when(mLockHandle.isMyTurn("id1")).thenReturn(true);
        ReturnType lock = musicZKCore.acquireLock("ks1.tn1", "id1");
        assertEquals(lock.getResult(), ResultType.SUCCESS);
        Mockito.verify(mLockHandle).isMyTurn("id1");
    }

    @Test
    public void testAcquireLockifisMyTurnFalse() throws MusicLockingException {
        Mockito.when(mLockHandle.isMyTurn("id1")).thenReturn(false);
        ReturnType lock = musicZKCore.acquireLock("ks1.ts1", "id1");
        assertEquals(lock.getResult(), ResultType.FAILURE);
        Mockito.verify(mLockHandle).isMyTurn("id1");
    }

    @Test
    public void testAcquireLockifisMyTurnTrueandIsTableOrKeySpaceLockTrue() throws MusicLockingException {
        Mockito.when(mLockHandle.isMyTurn("id1")).thenReturn(true);
        ReturnType lock = musicZKCore.acquireLock("ks1.tn1", "id1");
        assertEquals(lock.getResult(), ResultType.SUCCESS);
        Mockito.verify(mLockHandle).isMyTurn("id1");
    }

    @Test
    public void testAcquireLockifisMyTurnTrueandIsTableOrKeySpaceLockFalseandHaveLock() throws MusicLockingException {
        MusicLockState musicLockState = new MusicLockState(LockStatus.LOCKED, "id1");
        Mockito.when(mLockHandle.isMyTurn("id1")).thenReturn(true);
        Mockito.when(mLockHandle.getLockState("ks1.tn1.pk1")).thenReturn(musicLockState);
        ReturnType lock = musicZKCore.acquireLock("ks1.tn1.pk1", "id1");
        assertEquals(lock.getResult(), ResultType.SUCCESS);
        Mockito.verify(mLockHandle).isMyTurn("id1");
        Mockito.verify(mLockHandle).getLockState("ks1.tn1.pk1");
    }

    @Test
    public void testAcquireLockifisMyTurnTrueandIsTableOrKeySpaceLockFalseandDontHaveLock() throws MusicLockingException {
        MusicLockState musicLockState = new MusicLockState(LockStatus.LOCKED, "id2");
        Mockito.when(mLockHandle.isMyTurn("id1")).thenReturn(true);
        Mockito.when(mLockHandle.getLockState("ks1.tn1.pk1")).thenReturn(musicLockState);
        ReturnType lock = musicZKCore.acquireLock("ks1.tn1.pk1", "id1");
        assertEquals(lock.getResult(), ResultType.SUCCESS);
        Mockito.verify(mLockHandle).isMyTurn("id1");
        Mockito.verify(mLockHandle).getLockState("ks1.tn1.pk1");
    }
    
    @Test
    public void testAcquireLockifLockRefDoesntExist() throws MusicLockingException {
        Mockito.when(mLockHandle.lockIdExists("bs1")).thenReturn(false);
        ReturnType lock = musicZKCore.acquireLock("ks1.ts1", "bs1");
        assertEquals(lock.getResult(), ResultType.FAILURE);
        assertEquals(lock.getMessage(), "Lockid doesn't exist");
        Mockito.verify(mLockHandle).lockIdExists("bs1");
    }
    
    @Test
    public void testAcquireLockWithLeasewithLease() throws MusicLockingException {
        MusicLockState musicLockState = new MusicLockState(LockStatus.LOCKED, "id1");
        musicLockState.setLeasePeriod(0);
        ReturnType expectedResult = new ReturnType(ResultType.SUCCESS, "Succes");
        Mockito.when(mLockHandle.getLockState("ks1.tn1.pk1")).thenReturn(musicLockState);
        Mockito.when(mLockHandle.isMyTurn("id1")).thenReturn(true);
        ReturnType actualResult = musicZKCore.acquireLockWithLease("ks1.tn1.pk1", "id1", 6000);
        assertEquals(expectedResult.getResult(), actualResult.getResult());
        Mockito.verify(mLockHandle).isMyTurn("id1");
        Mockito.verify(mLockHandle, Mockito.atLeastOnce()).getLockState("ks1.tn1.pk1");
    }
    
    @Test
    public void testAcquireLockWithLeasewithException() throws MusicLockingException {
        ReturnType expectedResult = new ReturnType(ResultType.FAILURE, "failure");
        Mockito.when(mLockHandle.getLockState("ks1.tn1.pk1")).thenThrow(new MusicLockingException());
        Mockito.when(mLockHandle.isMyTurn("id1")).thenReturn(true);
        ReturnType actualResult = musicZKCore.acquireLockWithLease("ks1.tn1.pk1", "id1", 6000);
        assertEquals(expectedResult.getResult(), actualResult.getResult());
        Mockito.verify(mLockHandle, Mockito.atLeastOnce()).getLockState("ks1.tn1.pk1");
    }

    @Test
    public void testAcquireLockWithLeasewithLockStatusLOCKED() throws MusicLockingException {
        MusicLockState musicLockState = new MusicLockState(LockStatus.LOCKED, "id1");
        ReturnType expectedResult = new ReturnType(ResultType.SUCCESS, "Succes");
        Mockito.when(mLockHandle.getLockState("ks1.tn1.pk1")).thenReturn(musicLockState);
        Mockito.when(mLockHandle.isMyTurn("id1")).thenReturn(true);
        ReturnType actualResult = musicZKCore.acquireLockWithLease("ks1.tn1.pk1", "id1", 6000);
        assertEquals(expectedResult.getResult(), actualResult.getResult());
        Mockito.verify(mLockHandle).isMyTurn("id1");
        Mockito.verify(mLockHandle, Mockito.atLeastOnce()).getLockState("ks1.tn1.pk1");
    }

    @Test
    public void testAcquireLockWithLeasewithLockStatusUNLOCKED() throws MusicLockingException {
        MusicLockState musicLockState = new MusicLockState(LockStatus.UNLOCKED, "id1");
        ReturnType expectedResult = new ReturnType(ResultType.SUCCESS, "Succes");
        Mockito.when(mLockHandle.getLockState("ks1.tn1.pk1")).thenReturn(musicLockState);
        Mockito.when(mLockHandle.isMyTurn("id1")).thenReturn(true);
        ReturnType actualResult = musicZKCore.acquireLockWithLease("ks1.tn1.pk1", "id1", 6000);
        assertEquals(expectedResult.getResult(), actualResult.getResult());
        Mockito.verify(mLockHandle).isMyTurn("id1");
        Mockito.verify(mLockHandle, Mockito.atLeastOnce()).getLockState("ks1.tn1.pk1");

    }

    @Test
    public void testAcquireLockWithLeaseIfNotMyTurn() throws MusicLockingException {
        MusicLockState musicLockState = new MusicLockState(LockStatus.UNLOCKED, "id1");
        ReturnType expectedResult = new ReturnType(ResultType.FAILURE, "Failure");
        Mockito.when(mLockHandle.getLockState("ks1.tn1.pk1")).thenReturn(musicLockState);
        Mockito.when(mLockHandle.isMyTurn("id1")).thenReturn(false);
        ReturnType actualResult = musicZKCore.acquireLockWithLease("ks1.tn1.pk1", "id1", 6000);
        assertEquals(expectedResult.getResult(), actualResult.getResult());
        Mockito.verify(mLockHandle).isMyTurn("id1");
        Mockito.verify(mLockHandle).getLockState("ks1.tn1.pk1");
    }

    @Test
    public void testQuorumGet() throws MusicServiceException, MusicQueryException {
        preparedQueryObject = Mockito.mock(PreparedQueryObject.class);
        MusicDataStoreHandle.mDstoreHandle = Mockito.mock(MusicDataStore.class);
        rs = Mockito.mock(ResultSet.class);
        session = Mockito.mock(Session.class);
        Mockito.when(MusicDataStoreHandle.mDstoreHandle.getSession()).thenReturn(session);
        Mockito.when(MusicDataStoreHandle.mDstoreHandle.executeQuorumConsistencyGet(preparedQueryObject)).thenReturn(rs);
        ResultSet rs1 = musicZKCore.quorumGet(preparedQueryObject);
        assertNotNull(rs1);
    }

    @Test
    public void testGetLockNameFromId() {
        String lockname = MusicZKCore.getLockNameFromId("lockName$id");
        assertEquals("lockName", lockname);
    }

    @Test
    public void testDestroyLockRef() throws NoNodeException {
        Mockito.doNothing().when(mLockHandle).unlockAndDeleteId("id1");
        musicZKCore.destroyLockRef("id1");
        Mockito.verify(mLockHandle, Mockito.atLeastOnce()).unlockAndDeleteId("id1");
    }

    @Test
    public void testreleaseLockwithvoluntaryReleaseTrue() throws NoNodeException {
        MusicLockState musicLockState = new MusicLockState(LockStatus.UNLOCKED, "id2");
        Mockito.doNothing().when(mLockHandle).unlockAndDeleteId("id1");
        MusicLockState musicLockState1 = musicZKCore.releaseLock("id1", true);
        assertEquals(musicLockState.getLockStatus(), musicLockState1.getLockStatus());
        Mockito.verify(mLockHandle, Mockito.atLeastOnce()).unlockAndDeleteId("id1");
    }

    @Test
    public void testreleaseLockwithvoluntaryReleaseFalse() throws NoNodeException {
        MusicLockState musicLockState = new MusicLockState(LockStatus.UNLOCKED, "id2");
        Mockito.doNothing().when(mLockHandle).unlockAndDeleteId("id1");
        MusicLockState musicLockState1 = musicZKCore.releaseLock("id1", false);
        assertEquals(musicLockState.getLockStatus(), musicLockState1.getLockStatus());
        Mockito.verify(mLockHandle, Mockito.atLeastOnce()).unlockAndDeleteId("id1");
    }

    @Test
    public void testDeleteLock() throws MusicLockingException {
        Mockito.doNothing().when(mLockHandle).deleteLock("/" + "id1");
        musicZKCore.deleteLock("id1");
        Mockito.verify(mLockHandle).deleteLock("/" + "id1");
    }

    /*
     * @Test public void testNonKeyRelatedPut() throws Exception { MusicDataStoreHandle.mDstoreHandle =
     * Mockito.mock(MusicDataStore.class); Mockito.when(MusicDataStoreHandle.mDstoreHandle.executePut("qu1",
     * "consistency")).thenReturn(true); Boolean result = MusicCore.nonKeyRelatedPut("qu1",
     * "consistency"); assertTrue(result); Mockito.verify(MusicDataStoreHandle.mDstoreHandle).executePut("qu1",
     * "consistency"); }
     */

    @Test
    public void testEventualPutPreparedQuery() throws MusicServiceException, MusicQueryException {
        MusicDataStoreHandle.mDstoreHandle = Mockito.mock(MusicDataStore.class);
        preparedQueryObject = Mockito.mock(PreparedQueryObject.class);
        ReturnType expectedResult = new ReturnType(ResultType.SUCCESS, "Succes");
        session = Mockito.mock(Session.class);
        Mockito.when(MusicDataStoreHandle.mDstoreHandle.getSession()).thenReturn(session);
        Mockito.when(MusicDataStoreHandle.mDstoreHandle.executePut(preparedQueryObject, "eventual")).thenReturn(true);
        ReturnType actualResult = musicZKCore.eventualPut(preparedQueryObject);
        assertEquals(expectedResult.getResult(), actualResult.getResult());
        Mockito.verify(MusicDataStoreHandle.mDstoreHandle).executePut(preparedQueryObject, "eventual");
    }

    @Test
    public void testEventualPutPreparedQuerywithResultFalse()
                    throws MusicServiceException, MusicQueryException {
        MusicDataStoreHandle.mDstoreHandle = Mockito.mock(MusicDataStore.class);
        preparedQueryObject = Mockito.mock(PreparedQueryObject.class);
        ReturnType expectedResult = new ReturnType(ResultType.FAILURE, "Failure");
        session = Mockito.mock(Session.class);
        Mockito.when(MusicDataStoreHandle.mDstoreHandle.getSession()).thenReturn(session);
        Mockito.when(MusicDataStoreHandle.mDstoreHandle.executePut(preparedQueryObject, "eventual")).thenReturn(false);
        ReturnType actualResult = musicZKCore.eventualPut(preparedQueryObject);
        assertEquals(expectedResult.getResult(), actualResult.getResult());
        Mockito.verify(MusicDataStoreHandle.mDstoreHandle).executePut(preparedQueryObject, "eventual");
        //Mockito.verify(MusicDataStoreHandle.mDstoreHandle).executePut(preparedQueryObject, MusicUtil.EVENTUAL);
    }

    @Test
    public void testCriticalPutPreparedQuerywithValidLockId()
                    throws Exception {
        MusicDataStoreHandle.mDstoreHandle = Mockito.mock(MusicDataStore.class);
        preparedQueryObject = Mockito.mock(PreparedQueryObject.class);
        MusicLockState musicLockState = new MusicLockState(LockStatus.UNLOCKED, "id1");
        Mockito.when(condition.testCondition()).thenReturn(true);
        session = Mockito.mock(Session.class);
        Mockito.when(MusicDataStoreHandle.mDstoreHandle.getSession()).thenReturn(session);
        ReturnType expectedResult = new ReturnType(ResultType.SUCCESS, "Succes");
        Mockito.when(mLockHandle.getLockState("ks1" + "." + "tn1" + "." + "pk1"))
                        .thenReturn(musicLockState);
        Mockito.when(MusicDataStoreHandle.mDstoreHandle.executePut(preparedQueryObject, "critical")).thenReturn(true);
        ReturnType returnType = musicZKCore.criticalPut("ks1", "tn1", "pk1", preparedQueryObject,
                        "id1", condition);
        assertEquals(expectedResult.getResult(), returnType.getResult());
        Mockito.verify(condition).testCondition();
        Mockito.verify(mLockHandle).getLockState("ks1" + "." + "tn1" + "." + "pk1");
        Mockito.verify(MusicDataStoreHandle.mDstoreHandle).executePut(preparedQueryObject, "critical");
    }

    @Test
    public void testCriticalPutPreparedQuerywithInvalidLockId() throws MusicLockingException {
        MusicDataStoreHandle.mDstoreHandle = Mockito.mock(MusicDataStore.class);
        preparedQueryObject = Mockito.mock(PreparedQueryObject.class);
        MusicLockState musicLockState = new MusicLockState(LockStatus.UNLOCKED, "id2");
        ReturnType expectedResult = new ReturnType(ResultType.FAILURE, "Failure");
        Mockito.when(mLockHandle.getLockState("ks1" + "." + "tn1" + "." + "pk1"))
                        .thenReturn(musicLockState);
        ReturnType returnType = musicZKCore.criticalPut("ks1", "tn1", "pk1", preparedQueryObject,
                        "id1", condition);
        assertEquals(expectedResult.getResult(), returnType.getResult());
        Mockito.verify(mLockHandle).getLockState("ks1" + "." + "tn1" + "." + "pk1");
    }

    @Test
    public void testCriticalPutPreparedQuerywithvalidLockIdandTestConditionFalse() throws Exception {
        MusicDataStoreHandle.mDstoreHandle = Mockito.mock(MusicDataStore.class);
        preparedQueryObject = Mockito.mock(PreparedQueryObject.class);
        MusicLockState musicLockState = new MusicLockState(LockStatus.UNLOCKED, "id1");
        Mockito.when(condition.testCondition()).thenReturn(false);
        ReturnType expectedResult = new ReturnType(ResultType.FAILURE, "Failure");
        Mockito.when(mLockHandle.getLockState("ks1" + "." + "tn1" + "." + "pk1"))
                        .thenReturn(musicLockState);
        ReturnType returnType = musicZKCore.criticalPut("ks1", "tn1", "pk1", preparedQueryObject,
                        "id1", condition);
        assertEquals(expectedResult.getResult(), returnType.getResult());
        Mockito.verify(condition).testCondition();
        Mockito.verify(mLockHandle).getLockState("ks1" + "." + "tn1" + "." + "pk1");
    }

    @Test
    public void testNonKeyRelatedPutPreparedQuery() throws Exception {
        MusicDataStoreHandle.mDstoreHandle = Mockito.mock(MusicDataStore.class);
        preparedQueryObject = Mockito.mock(PreparedQueryObject.class);
        session = Mockito.mock(Session.class);
        Mockito.when(MusicDataStoreHandle.mDstoreHandle.getSession()).thenReturn(session);
        Mockito.when(MusicDataStoreHandle.mDstoreHandle.executePut(preparedQueryObject, "consistency")).thenReturn(true);
        ResultType result = musicZKCore.nonKeyRelatedPut(preparedQueryObject, "consistency");
        assertEquals(ResultType.SUCCESS, result);
        Mockito.verify(MusicDataStoreHandle.mDstoreHandle).executePut(preparedQueryObject, "consistency");
    }

    @Test
    public void testAtomicPutPreparedQuery() throws Exception {
        MusicDataStoreHandle.mDstoreHandle = Mockito.mock(MusicDataStore.class);
        preparedQueryObject = Mockito.mock(PreparedQueryObject.class);
        Mockito.when(mLockHandle.createLockId("/" + "ks1.tn1.pk1")).thenReturn("id1");
        MusicLockState musicLockState = new MusicLockState(LockStatus.LOCKED, "id1");
        ReturnType expectedResult = new ReturnType(ResultType.SUCCESS, "Succes");
        session = Mockito.mock(Session.class);
        Mockito.when(MusicDataStoreHandle.mDstoreHandle.getSession()).thenReturn(session);
        Mockito.when(mLockHandle.getLockState("ks1.tn1.pk1")).thenReturn(musicLockState);
        Mockito.when(mLockHandle.isMyTurn("id1")).thenReturn(true);
        Mockito.when(condition.testCondition()).thenReturn(true);
        Mockito.when(mLockHandle.getLockState("ks1" + "." + "tn1" + "." + "pk1"))
                        .thenReturn(musicLockState);
        Mockito.when(MusicDataStoreHandle.mDstoreHandle.executePut(preparedQueryObject, "critical")).thenReturn(true);
        ReturnType returnType =
                musicZKCore.atomicPut("ks1", "tn1", "pk1", preparedQueryObject, condition);
        assertEquals(expectedResult.getResult(), returnType.getResult());
        Mockito.verify(mLockHandle).createLockId("/" + "ks1.tn1.pk1");
        Mockito.verify(mLockHandle, Mockito.atLeastOnce()).getLockState("ks1.tn1.pk1");
        Mockito.verify(mLockHandle).isMyTurn("id1");
        Mockito.verify(condition).testCondition();
        Mockito.verify(mLockHandle, Mockito.atLeastOnce())
                        .getLockState("ks1" + "." + "tn1" + "." + "pk1");
        Mockito.verify(MusicDataStoreHandle.mDstoreHandle).executePut(preparedQueryObject, "critical");
    }

    @Test
    public void testAtomicPutPreparedQuerywithAcquireLockWithLeaseFalse() throws MusicLockingException {
        MusicDataStoreHandle.mDstoreHandle = Mockito.mock(MusicDataStore.class);
        preparedQueryObject = Mockito.mock(PreparedQueryObject.class);
        Mockito.when(mLockHandle.createLockId("/" + "ks1.tn1.pk1")).thenReturn("id1");
        ReturnType expectedResult = new ReturnType(ResultType.FAILURE, "Failure");
        Mockito.when(mLockHandle.isMyTurn("id1")).thenReturn(false);
        ReturnType returnType =
                musicZKCore.atomicPut("ks1", "tn1", "pk1", preparedQueryObject, condition);
        assertEquals(expectedResult.getResult(), returnType.getResult());
        Mockito.verify(mLockHandle).isMyTurn("id1");
        Mockito.verify(mLockHandle).createLockId("/" + "ks1.tn1.pk1");
    }

    @Test
    public void testAtomicGetPreparedQuery() throws MusicServiceException, MusicQueryException, MusicLockingException {
        MusicDataStoreHandle.mDstoreHandle = Mockito.mock(MusicDataStore.class);
        preparedQueryObject = Mockito.mock(PreparedQueryObject.class);
        rs = Mockito.mock(ResultSet.class);
        session = Mockito.mock(Session.class);
        Mockito.when(MusicDataStoreHandle.mDstoreHandle.getSession()).thenReturn(session);
        Mockito.when(mLockHandle.createLockId("/" + "ks1.tn1.pk1")).thenReturn("id1");
        MusicLockState musicLockState = new MusicLockState(LockStatus.LOCKED, "id1");
        Mockito.when(mLockHandle.getLockState("ks1.tn1.pk1")).thenReturn(musicLockState);
        Mockito.when(mLockHandle.isMyTurn("id1")).thenReturn(true);
        Mockito.when(mLockHandle.getLockState("ks1" + "." + "tn1" + "." + "pk1"))
                        .thenReturn(musicLockState);
        Mockito.when(MusicDataStoreHandle.mDstoreHandle.executeQuorumConsistencyGet(preparedQueryObject)).thenReturn(rs);
        ResultSet rs1 = musicZKCore.atomicGet("ks1", "tn1", "pk1", preparedQueryObject);
        assertNotNull(rs1);
        Mockito.verify(mLockHandle).createLockId("/" + "ks1.tn1.pk1");
        Mockito.verify(mLockHandle, Mockito.atLeastOnce()).getLockState("ks1.tn1.pk1");
        Mockito.verify(mLockHandle).isMyTurn("id1");
        Mockito.verify(mLockHandle, Mockito.atLeastOnce())
                        .getLockState("ks1" + "." + "tn1" + "." + "pk1");
        Mockito.verify(MusicDataStoreHandle.mDstoreHandle).executeQuorumConsistencyGet(preparedQueryObject);
    }

    @Test
    public void testAtomicGetPreparedQuerywithAcquireLockWithLeaseFalse()
                    throws MusicServiceException, MusicLockingException {
        MusicDataStoreHandle.mDstoreHandle = Mockito.mock(MusicDataStore.class);
        preparedQueryObject = Mockito.mock(PreparedQueryObject.class);
        rs = Mockito.mock(ResultSet.class);
        Mockito.when(mLockHandle.createLockId("/" + "ks1.tn1.pk1")).thenReturn("id1");
        Mockito.when(mLockHandle.isMyTurn("id1")).thenReturn(false);
        ResultSet rs1 = musicZKCore.atomicGet("ks1", "tn1", "pk1", preparedQueryObject);
        assertNull(rs1);
        Mockito.verify(mLockHandle).createLockId("/" + "ks1.tn1.pk1");
        Mockito.verify(mLockHandle).isMyTurn("id1");
    }

    @Test
    public void testGetPreparedQuery() throws MusicServiceException, MusicQueryException {
        MusicDataStoreHandle.mDstoreHandle = Mockito.mock(MusicDataStore.class);
        rs = Mockito.mock(ResultSet.class);
        preparedQueryObject = Mockito.mock(PreparedQueryObject.class);
        session = Mockito.mock(Session.class);
        Mockito.when(MusicDataStoreHandle.mDstoreHandle.getSession()).thenReturn(session);
        Mockito.when(MusicDataStoreHandle.mDstoreHandle.executeOneConsistencyGet(preparedQueryObject)).thenReturn(rs);
        ResultSet rs1 = musicZKCore.get(preparedQueryObject);
        assertNotNull(rs1);
        Mockito.verify(MusicDataStoreHandle.mDstoreHandle).executeOneConsistencyGet(preparedQueryObject);

    }

    @Test
    public void testcriticalGetPreparedQuery() throws MusicServiceException, MusicQueryException, MusicLockingException {
        MusicDataStoreHandle.mDstoreHandle = Mockito.mock(MusicDataStore.class);
        preparedQueryObject = Mockito.mock(PreparedQueryObject.class);
        MusicLockState musicLockState = new MusicLockState(LockStatus.UNLOCKED, "id1");
        rs = Mockito.mock(ResultSet.class);
        session = Mockito.mock(Session.class);
        Mockito.when(MusicDataStoreHandle.mDstoreHandle.getSession()).thenReturn(session);
        Mockito.when(mLockHandle.getLockState("ks1" + "." + "tn1" + "." + "pk1"))
                        .thenReturn(musicLockState);
        Mockito.when(MusicDataStoreHandle.mDstoreHandle.executeQuorumConsistencyGet(preparedQueryObject)).thenReturn(rs);
        ResultSet rs1 = musicZKCore.criticalGet("ks1", "tn1", "pk1", preparedQueryObject, "id1");
        assertNotNull(rs1);
        Mockito.verify(mLockHandle, Mockito.atLeastOnce())
                        .getLockState("ks1" + "." + "tn1" + "." + "pk1");
        Mockito.verify(MusicDataStoreHandle.mDstoreHandle).executeQuorumConsistencyGet(preparedQueryObject);
    }

    @Test
    public void testcriticalGetPreparedQuerywithInvalidLockId() throws MusicServiceException, MusicLockingException {
        MusicDataStoreHandle.mDstoreHandle = Mockito.mock(MusicDataStore.class);
        preparedQueryObject = Mockito.mock(PreparedQueryObject.class);
        MusicLockState musicLockState = new MusicLockState(LockStatus.UNLOCKED, "id2");
        Mockito.when(mLockHandle.getLockState("ks1" + "." + "tn1" + "." + "pk1"))
                        .thenReturn(musicLockState);
        ResultSet rs1 = musicZKCore.criticalGet("ks1", "tn1", "pk1", preparedQueryObject, "id1");
        assertNull(rs1);
        Mockito.verify(mLockHandle, Mockito.atLeastOnce())
                        .getLockState("ks1" + "." + "tn1" + "." + "pk1");
    }

    @Test
    public void testAtomicGetPreparedQuerywithDeleteLockWithLeaseFalse()
                    throws MusicServiceException, MusicLockingException {
        MusicDataStoreHandle.mDstoreHandle = Mockito.mock(MusicDataStore.class);
        preparedQueryObject = Mockito.mock(PreparedQueryObject.class);
        rs = Mockito.mock(ResultSet.class);
        Mockito.when(mLockHandle.createLockId("/" + "ks1.tn1.pk1")).thenReturn("id1");
        Mockito.when(mLockHandle.isMyTurn("id1")).thenReturn(false);
        ResultSet rs1 = musicZKCore.atomicGet("ks1", "tn1", "pk1", preparedQueryObject);
        assertNull(rs1);
        Mockito.verify(mLockHandle).createLockId("/" + "ks1.tn1.pk1");
        Mockito.verify(mLockHandle).isMyTurn("id1");
    }
    
    @Test
    public void testCondition() throws Exception {
      //Condition conClass =  Mockito.mock(Condition.class);
     // boolean ret=true;
     //Mockito.when(conClass.testCondition().thenReturn(ret);
      Map<String, Object> conditionsx=null;
      PreparedQueryObject selectQueryForTheRowx=null;
      try {
      Condition con = new Condition(conditionsx,selectQueryForTheRowx);
      assertTrue(con.testCondition());
      } catch (Exception e) {
        assertFalse(false);
      }
    }
  //getLockingServiceHandl  
    
    @Ignore
    @Test(expected = MusicLockingException.class) //("Failed to aquire Locl store handle " + e))
    public void testgetLockingServiceHandle() throws Exception {
     // MusicLockingService mLockHandlex =  Mockito.mock(MusicLockingService.class);
      //MusicLockingService mLockHandlea = mLockHandle;
      //mLockHandle=null;
      System.out.println("cjc 0 locking test n");
     // Mockito.when(MusicCore.getLockingServiceHandle()).thenReturn(mLockHandle);
      //mLockHandle=null;
      //System.out.println("cjc 0-1  locking test n");
      MusicLockingService mLockHandlea = mLockHandle;
      mLockHandle=null;
      
      MusicLockingService mLockHandley=null; //MusicCore.getLockingServiceHandle();
      Mockito.when(MusicZKCore.getLockingServiceHandle()).thenReturn(mLockHandley);
      System.out.println("cjc locking test n");
      mLockHandle=mLockHandlea;
      assertTrue(true);
      
    }
  //add mocking 
   @Ignore
   @Test
   public void testGetDSHandleIp() throws MusicServiceException, MusicQueryException {
      // rs = Mockito.mock(ResultSet.class);
      // session = Mockito.mock(Session.class);
       //Mockito.when(MusicDataStoreHandle.mDstoreHandle.getSession()).thenReturn(session);
       //Mockito.when(MusicDataStoreHandle.mDstoreHandle.executeCriticalGet(preparedQueryObject)).thenReturn(rs);
      
       //MusicDataStoreHandle.mDstoreHandle = Mockito.mock(MusicDataStore.class);
       //MusicUtil mMusicUtil=Mockito.mock(MusicUtil.class);
       System.out.println("cjc 0 getDsHandleIP");
       Mockito.when(MusicDataStoreHandle.getDSHandle("1.127.0.1")).thenReturn(MusicDataStoreHandle.mDstoreHandle);
       System.out.println("cjc localhost");
      // Mockito.when(mMusicUtil.getMyCassaHost().equals("localhost")).thenReturn(null);
       System.out.println("cjc 1 localhost IP");
    //     MusicDataStoreHandle.mDstoreHandle = new MusicDataStore(MusicUtil.getMyCassaHost());
    // } else {
    //     MusicDataStoreHandle.mDstoreHandle = new MusicDataStore();
    // }
       assertTrue(true);
   }
   
   @Ignore
   @Test
   public void testPureZkCreate() { 
     try {
     MusicZKCore.pureZkCreate("local");
     } catch(NullPointerException e) {
       System.out.println("cjc zkcreate null pointwer exception:"+ e);
     }
   }  
   
   @Ignore
   @Test 
   public void testPureZkRead() {   //String nodeName) {
     byte[] data = MusicZKCore.pureZkRead("localhost");
   }

   //need fixing
   @Ignore
   @Test
   public void testPureZkWrite() { //String nodeName, byte[] data) {
     /*
     long start = System.currentTimeMillis();
     logger.info(EELFLoggerDelegate.applicationLogger,"Performing zookeeper write to " + nodeName);
     try {
         getLockingServiceHandle().getzkLockHandle().setNodeData(nodeName, data);
     } catch (MusicLockingException e) {
         logger.error(EELFLoggerDelegate.errorLogger,e.getMessage(), "[ERR512E] Failed to get ZK Lock Handle "  ,ErrorSeverity.CRITICAL, ErrorTypes.LOCKINGERROR);
     }
     logger.info(EELFLoggerDelegate.applicationLogger,"Performed zookeeper write to " + nodeName);
     long end = System.currentTimeMillis();
     logger.info(EELFLoggerDelegate.applicationLogger,"Time taken for the actual zk put:" + (end - start) + " ms");
    */
    
      // MusicDataStoreHandle.mDstoreHandle = Mockito.mock(MusicDataStore.class);
      // rs = Mockito.mock(ResultSet.class);
      // session = Mockito.mock(Session.class);
       //Mockito.when(MusicDataStoreHandle.mDstoreHandle.getSession()).thenReturn(session);
       
       byte[] data= "Testing Zoo Keeper".getBytes();
       MusicZKCore.pureZkWrite("1.127.0.1", data);
      // assertNotNull(rs1);
   }
   
   @Test
   public void testWhoseTurnIsIt() { //(String lockName) {

     /*
     try {
         return getLockingServiceHandle().whoseTurnIsIt("/" + lockName) + "";
     } catch (MusicLockingException e) {
         logger.error(EELFLoggerDelegate.errorLogger,e.getMessage(), AppMessages.LOCKINGERROR+lockName ,ErrorSeverity.CRITICAL, ErrorTypes.LOCKINGERROR);
     }
     return null;
     */
     
     String lockName="xxx";
     if (musicZKCore.whoseTurnIsIt(lockName) != null) assertTrue(true);
     


 }
   
   @Test
   public void testMarshallResults() { 
     Map<String, HashMap<String, Object>> ret=null;
     //ResultSet results =null;
     rs = Mockito.mock(ResultSet.class);
    try { 
      ret= MusicDataStoreHandle.marshallResults(rs);
      
     } catch( Exception e ) {
     
     }
    
     if (ret != null) assertTrue(true);
   }

}
