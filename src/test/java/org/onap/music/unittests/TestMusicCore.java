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
import static org.onap.music.main.MusicCore.mDstoreHandle;
import static org.onap.music.main.MusicCore.mLockHandle;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.onap.music.exceptions.MusicLockingException;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.lockingservice.MusicLockState;
import org.onap.music.lockingservice.MusicLockingService;
import org.onap.music.lockingservice.MusicLockState.LockStatus;
import org.onap.music.main.MusicCore;
import org.onap.music.main.MusicUtil;
import org.onap.music.main.ResultType;
import org.onap.music.main.ReturnType;
import org.onap.music.main.MusicCore.Condition;
import org.onap.music.datastore.MusicDataStore;
import org.onap.music.datastore.PreparedQueryObject;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

@RunWith(MockitoJUnitRunner.class)
public class TestMusicCore {

    @Mock
    private Condition condition;

    @Mock
    private ResultSet rs;

    @Mock
    private PreparedQueryObject preparedQueryObject;
    
    @Mock
    private Session session;

    @Before
    public void setUp() {
        mLockHandle = Mockito.mock(MusicLockingService.class);

    }

    @Test
    public void testCreateLockReferenceforvalidlock() {
        Mockito.when(mLockHandle.createLockId("/" + "test")).thenReturn("lock");
        String lockId = MusicCore.createLockReference("test");
        assertEquals("lock", lockId);
        Mockito.verify(mLockHandle).createLockId("/" + "test");
    }

    @Test
    public void testIsTableOrKeySpaceLock() {
        Boolean result = MusicCore.isTableOrKeySpaceLock("ks1.tn1");
        assertTrue(result);
    }

    @Test
    public void testIsTableOrKeySpaceLockwithPrimarykey() {
        Boolean result = MusicCore.isTableOrKeySpaceLock("ks1.tn1.pk1");
        assertFalse(result);
    }

    @Test
    public void testGetMusicLockState() throws MusicLockingException {
        MusicLockState musicLockState = new MusicLockState(LockStatus.UNLOCKED, "id1");
        Mockito.when(mLockHandle.getLockState("ks1.tb1.pk1")).thenReturn(musicLockState);
        MusicLockState mls = MusicCore.getMusicLockState("ks1.tb1.pk1");
        assertEquals(musicLockState, mls);
        Mockito.verify(mLockHandle).getLockState("ks1.tb1.pk1");
    }

    @Test
    public void testAcquireLockifisMyTurnTrue() {
        Mockito.when(mLockHandle.isMyTurn("id1")).thenReturn(true);
        ReturnType lock = MusicCore.acquireLock("ks1.tn1", "id1");
        assertEquals(lock.getResult(), ResultType.SUCCESS);
        Mockito.verify(mLockHandle).isMyTurn("id1");
    }

    @Test
    public void testAcquireLockifisMyTurnFalse() {
        Mockito.when(mLockHandle.isMyTurn("id1")).thenReturn(false);
        ReturnType lock = MusicCore.acquireLock("ks1.ts1", "id1");
        assertEquals(lock.getResult(), ResultType.FAILURE);
        Mockito.verify(mLockHandle).isMyTurn("id1");
    }

    @Test
    public void testAcquireLockifisMyTurnTrueandIsTableOrKeySpaceLockTrue() {
        Mockito.when(mLockHandle.isMyTurn("id1")).thenReturn(true);
        ReturnType lock = MusicCore.acquireLock("ks1.tn1", "id1");
        assertEquals(lock.getResult(), ResultType.SUCCESS);
        Mockito.verify(mLockHandle).isMyTurn("id1");
    }

    @Test
    public void testAcquireLockifisMyTurnTrueandIsTableOrKeySpaceLockFalseandHaveLock() throws MusicLockingException {
        MusicLockState musicLockState = new MusicLockState(LockStatus.LOCKED, "id1");
        Mockito.when(mLockHandle.isMyTurn("id1")).thenReturn(true);
        Mockito.when(mLockHandle.getLockState("ks1.tn1.pk1")).thenReturn(musicLockState);
        ReturnType lock = MusicCore.acquireLock("ks1.tn1.pk1", "id1");
        assertEquals(lock.getResult(), ResultType.SUCCESS);
        Mockito.verify(mLockHandle).isMyTurn("id1");
        Mockito.verify(mLockHandle).getLockState("ks1.tn1.pk1");
    }

    @Test
    public void testAcquireLockifisMyTurnTrueandIsTableOrKeySpaceLockFalseandDontHaveLock() throws MusicLockingException {
        MusicLockState musicLockState = new MusicLockState(LockStatus.LOCKED, "id2");
        Mockito.when(mLockHandle.isMyTurn("id1")).thenReturn(true);
        Mockito.when(mLockHandle.getLockState("ks1.tn1.pk1")).thenReturn(musicLockState);
        ReturnType lock = MusicCore.acquireLock("ks1.tn1.pk1", "id1");
        assertEquals(lock.getResult(), ResultType.SUCCESS);
        Mockito.verify(mLockHandle).isMyTurn("id1");
        Mockito.verify(mLockHandle).getLockState("ks1.tn1.pk1");
    }
    
    @Test
    public void testAcquireLockifLockRefDoesntExist() {
        Mockito.when(mLockHandle.lockIdExists("bs1")).thenReturn(false);
        ReturnType lock = MusicCore.acquireLock("ks1.ts1", "bs1");
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
        ReturnType actualResult = MusicCore.acquireLockWithLease("ks1.tn1.pk1", "id1", 6000);
        assertEquals(expectedResult.getResult(), actualResult.getResult());
        Mockito.verify(mLockHandle).isMyTurn("id1");
        Mockito.verify(mLockHandle, Mockito.atLeastOnce()).getLockState("ks1.tn1.pk1");
    }
    
    @Test
    public void testAcquireLockWithLeasewithException() throws MusicLockingException {
        ReturnType expectedResult = new ReturnType(ResultType.FAILURE, "failure");
        Mockito.when(mLockHandle.getLockState("ks1.tn1.pk1")).thenThrow(new MusicLockingException());
        Mockito.when(mLockHandle.isMyTurn("id1")).thenReturn(true);
        ReturnType actualResult = MusicCore.acquireLockWithLease("ks1.tn1.pk1", "id1", 6000);
        assertEquals(expectedResult.getResult(), actualResult.getResult());
        Mockito.verify(mLockHandle, Mockito.atLeastOnce()).getLockState("ks1.tn1.pk1");
    }

    @Test
    public void testAcquireLockWithLeasewithLockStatusLOCKED() throws MusicLockingException {
        MusicLockState musicLockState = new MusicLockState(LockStatus.LOCKED, "id1");
        ReturnType expectedResult = new ReturnType(ResultType.SUCCESS, "Succes");
        Mockito.when(mLockHandle.getLockState("ks1.tn1.pk1")).thenReturn(musicLockState);
        Mockito.when(mLockHandle.isMyTurn("id1")).thenReturn(true);
        ReturnType actualResult = MusicCore.acquireLockWithLease("ks1.tn1.pk1", "id1", 6000);
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
        ReturnType actualResult = MusicCore.acquireLockWithLease("ks1.tn1.pk1", "id1", 6000);
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
        ReturnType actualResult = MusicCore.acquireLockWithLease("ks1.tn1.pk1", "id1", 6000);
        assertEquals(expectedResult.getResult(), actualResult.getResult());
        Mockito.verify(mLockHandle).isMyTurn("id1");
        Mockito.verify(mLockHandle).getLockState("ks1.tn1.pk1");
    }

    @Test
    public void testQuorumGet() throws MusicServiceException, MusicQueryException {
        preparedQueryObject = Mockito.mock(PreparedQueryObject.class);
        mDstoreHandle = Mockito.mock(MusicDataStore.class);
        rs = Mockito.mock(ResultSet.class);
        session = Mockito.mock(Session.class);
        Mockito.when(mDstoreHandle.getSession()).thenReturn(session);
        Mockito.when(mDstoreHandle.executeCriticalGet(preparedQueryObject)).thenReturn(rs);
        ResultSet rs1 = MusicCore.quorumGet(preparedQueryObject);
        assertNotNull(rs1);
    }

    @Test
    public void testGetLockNameFromId() {
        String lockname = MusicCore.getLockNameFromId("lockName$id");
        assertEquals("lockName", lockname);
    }

    @Test
    public void testDestroyLockRef() {
        Mockito.doNothing().when(mLockHandle).unlockAndDeleteId("id1");
        MusicCore.destroyLockRef("id1");
        Mockito.verify(mLockHandle, Mockito.atLeastOnce()).unlockAndDeleteId("id1");
    }

    @Test
    public void testreleaseLockwithvoluntaryReleaseTrue() {
        MusicLockState musicLockState = new MusicLockState(LockStatus.UNLOCKED, "id2");
        Mockito.doNothing().when(mLockHandle).unlockAndDeleteId("id1");
        MusicLockState musicLockState1 = MusicCore.releaseLock("id1", true);
        assertEquals(musicLockState.getLockStatus(), musicLockState1.getLockStatus());
        Mockito.verify(mLockHandle, Mockito.atLeastOnce()).unlockAndDeleteId("id1");
    }

    @Test
    public void testreleaseLockwithvoluntaryReleaseFalse() {
        MusicLockState musicLockState = new MusicLockState(LockStatus.UNLOCKED, "id2");
        Mockito.doNothing().when(mLockHandle).unlockAndDeleteId("id1");
        MusicLockState musicLockState1 = MusicCore.releaseLock("id1", false);
        assertEquals(musicLockState.getLockStatus(), musicLockState1.getLockStatus());
        Mockito.verify(mLockHandle, Mockito.atLeastOnce()).unlockAndDeleteId("id1");
    }

    @Test
    public void testDeleteLock() {
        Mockito.doNothing().when(mLockHandle).deleteLock("/" + "id1");
        MusicCore.deleteLock("id1");
        Mockito.verify(mLockHandle).deleteLock("/" + "id1");
    }

    /*
     * @Test public void testNonKeyRelatedPut() throws Exception { mDstoreHandle =
     * Mockito.mock(MusicDataStore.class); Mockito.when(mDstoreHandle.executePut("qu1",
     * "consistency")).thenReturn(true); Boolean result = MusicCore.nonKeyRelatedPut("qu1",
     * "consistency"); assertTrue(result); Mockito.verify(mDstoreHandle).executePut("qu1",
     * "consistency"); }
     */

    @Test
    public void testEventualPutPreparedQuery() throws MusicServiceException, MusicQueryException {
        mDstoreHandle = Mockito.mock(MusicDataStore.class);
        preparedQueryObject = Mockito.mock(PreparedQueryObject.class);
        ReturnType expectedResult = new ReturnType(ResultType.SUCCESS, "Succes");
        session = Mockito.mock(Session.class);
        Mockito.when(mDstoreHandle.getSession()).thenReturn(session);
        Mockito.when(mDstoreHandle.executePut(preparedQueryObject, "eventual")).thenReturn(true);
        ReturnType actualResult = MusicCore.eventualPut(preparedQueryObject);
        assertEquals(expectedResult.getResult(), actualResult.getResult());
        Mockito.verify(mDstoreHandle).executePut(preparedQueryObject, "eventual");
    }

    @Test
    public void testEventualPutPreparedQuerywithResultFalse()
                    throws MusicServiceException, MusicQueryException {
        mDstoreHandle = Mockito.mock(MusicDataStore.class);
        preparedQueryObject = Mockito.mock(PreparedQueryObject.class);
        ReturnType expectedResult = new ReturnType(ResultType.FAILURE, "Failure");
        session = Mockito.mock(Session.class);
        Mockito.when(mDstoreHandle.getSession()).thenReturn(session);
        Mockito.when(mDstoreHandle.executePut(preparedQueryObject, "eventual")).thenReturn(false);
        ReturnType actualResult = MusicCore.eventualPut(preparedQueryObject);
        assertEquals(expectedResult.getResult(), actualResult.getResult());
        Mockito.verify(mDstoreHandle).executePut(preparedQueryObject, "eventual");
        //Mockito.verify(mDstoreHandle).executePut(preparedQueryObject, MusicUtil.EVENTUAL);
    }

    @Test
    public void testCriticalPutPreparedQuerywithValidLockId()
                    throws Exception {
        mDstoreHandle = Mockito.mock(MusicDataStore.class);
        preparedQueryObject = Mockito.mock(PreparedQueryObject.class);
        MusicLockState musicLockState = new MusicLockState(LockStatus.UNLOCKED, "id1");
        Mockito.when(condition.testCondition()).thenReturn(true);
        session = Mockito.mock(Session.class);
        Mockito.when(mDstoreHandle.getSession()).thenReturn(session);
        ReturnType expectedResult = new ReturnType(ResultType.SUCCESS, "Succes");
        Mockito.when(mLockHandle.getLockState("ks1" + "." + "tn1" + "." + "pk1"))
                        .thenReturn(musicLockState);
        Mockito.when(mDstoreHandle.executePut(preparedQueryObject, "critical")).thenReturn(true);
        ReturnType returnType = MusicCore.criticalPut("ks1", "tn1", "pk1", preparedQueryObject,
                        "id1", condition);
        assertEquals(expectedResult.getResult(), returnType.getResult());
        Mockito.verify(condition).testCondition();
        Mockito.verify(mLockHandle).getLockState("ks1" + "." + "tn1" + "." + "pk1");
        Mockito.verify(mDstoreHandle).executePut(preparedQueryObject, "critical");
    }

    @Test
    public void testCriticalPutPreparedQuerywithInvalidLockId() throws MusicLockingException {
        mDstoreHandle = Mockito.mock(MusicDataStore.class);
        preparedQueryObject = Mockito.mock(PreparedQueryObject.class);
        MusicLockState musicLockState = new MusicLockState(LockStatus.UNLOCKED, "id2");
        ReturnType expectedResult = new ReturnType(ResultType.FAILURE, "Failure");
        Mockito.when(mLockHandle.getLockState("ks1" + "." + "tn1" + "." + "pk1"))
                        .thenReturn(musicLockState);
        ReturnType returnType = MusicCore.criticalPut("ks1", "tn1", "pk1", preparedQueryObject,
                        "id1", condition);
        assertEquals(expectedResult.getResult(), returnType.getResult());
        Mockito.verify(mLockHandle).getLockState("ks1" + "." + "tn1" + "." + "pk1");
    }

    @Test
    public void testCriticalPutPreparedQuerywithvalidLockIdandTestConditionFalse() throws Exception {
        mDstoreHandle = Mockito.mock(MusicDataStore.class);
        preparedQueryObject = Mockito.mock(PreparedQueryObject.class);
        MusicLockState musicLockState = new MusicLockState(LockStatus.UNLOCKED, "id1");
        Mockito.when(condition.testCondition()).thenReturn(false);
        ReturnType expectedResult = new ReturnType(ResultType.FAILURE, "Failure");
        Mockito.when(mLockHandle.getLockState("ks1" + "." + "tn1" + "." + "pk1"))
                        .thenReturn(musicLockState);
        ReturnType returnType = MusicCore.criticalPut("ks1", "tn1", "pk1", preparedQueryObject,
                        "id1", condition);
        assertEquals(expectedResult.getResult(), returnType.getResult());
        Mockito.verify(condition).testCondition();
        Mockito.verify(mLockHandle).getLockState("ks1" + "." + "tn1" + "." + "pk1");
    }

    @Test
    public void testNonKeyRelatedPutPreparedQuery() throws Exception {
        mDstoreHandle = Mockito.mock(MusicDataStore.class);
        preparedQueryObject = Mockito.mock(PreparedQueryObject.class);
        session = Mockito.mock(Session.class);
        Mockito.when(mDstoreHandle.getSession()).thenReturn(session);
        Mockito.when(mDstoreHandle.executePut(preparedQueryObject, "consistency")).thenReturn(true);
        Boolean result = MusicCore.nonKeyRelatedPut(preparedQueryObject, "consistency");
        assertTrue(result);
        Mockito.verify(mDstoreHandle).executePut(preparedQueryObject, "consistency");
    }

    @Test
    public void testAtomicPutPreparedQuery() throws Exception {
        mDstoreHandle = Mockito.mock(MusicDataStore.class);
        preparedQueryObject = Mockito.mock(PreparedQueryObject.class);
        Mockito.when(mLockHandle.createLockId("/" + "ks1.tn1.pk1")).thenReturn("id1");
        MusicLockState musicLockState = new MusicLockState(LockStatus.LOCKED, "id1");
        ReturnType expectedResult = new ReturnType(ResultType.SUCCESS, "Succes");
        session = Mockito.mock(Session.class);
        Mockito.when(mDstoreHandle.getSession()).thenReturn(session);
        Mockito.when(mLockHandle.getLockState("ks1.tn1.pk1")).thenReturn(musicLockState);
        Mockito.when(mLockHandle.isMyTurn("id1")).thenReturn(true);
        Mockito.when(condition.testCondition()).thenReturn(true);
        Mockito.when(mLockHandle.getLockState("ks1" + "." + "tn1" + "." + "pk1"))
                        .thenReturn(musicLockState);
        Mockito.when(mDstoreHandle.executePut(preparedQueryObject, "critical")).thenReturn(true);
        ReturnType returnType =
                        MusicCore.atomicPut("ks1", "tn1", "pk1", preparedQueryObject, condition);
        assertEquals(expectedResult.getResult(), returnType.getResult());
        Mockito.verify(mLockHandle).createLockId("/" + "ks1.tn1.pk1");
        Mockito.verify(mLockHandle, Mockito.atLeastOnce()).getLockState("ks1.tn1.pk1");
        Mockito.verify(mLockHandle).isMyTurn("id1");
        Mockito.verify(condition).testCondition();
        Mockito.verify(mLockHandle, Mockito.atLeastOnce())
                        .getLockState("ks1" + "." + "tn1" + "." + "pk1");
        Mockito.verify(mDstoreHandle).executePut(preparedQueryObject, "critical");
    }

    @Test
    public void testAtomicPutPreparedQuerywithAcquireLockWithLeaseFalse() throws MusicLockingException {
        mDstoreHandle = Mockito.mock(MusicDataStore.class);
        preparedQueryObject = Mockito.mock(PreparedQueryObject.class);
        Mockito.when(mLockHandle.createLockId("/" + "ks1.tn1.pk1")).thenReturn("id1");
        ReturnType expectedResult = new ReturnType(ResultType.FAILURE, "Failure");
        Mockito.when(mLockHandle.isMyTurn("id1")).thenReturn(false);
        ReturnType returnType =
                        MusicCore.atomicPut("ks1", "tn1", "pk1", preparedQueryObject, condition);
        assertEquals(expectedResult.getResult(), returnType.getResult());
        Mockito.verify(mLockHandle).isMyTurn("id1");
        Mockito.verify(mLockHandle).createLockId("/" + "ks1.tn1.pk1");
    }

    @Test
    public void testAtomicGetPreparedQuery() throws MusicServiceException, MusicQueryException, MusicLockingException {
        mDstoreHandle = Mockito.mock(MusicDataStore.class);
        preparedQueryObject = Mockito.mock(PreparedQueryObject.class);
        rs = Mockito.mock(ResultSet.class);
        session = Mockito.mock(Session.class);
        Mockito.when(mDstoreHandle.getSession()).thenReturn(session);
        Mockito.when(mLockHandle.createLockId("/" + "ks1.tn1.pk1")).thenReturn("id1");
        MusicLockState musicLockState = new MusicLockState(LockStatus.LOCKED, "id1");
        Mockito.when(mLockHandle.getLockState("ks1.tn1.pk1")).thenReturn(musicLockState);
        Mockito.when(mLockHandle.isMyTurn("id1")).thenReturn(true);
        Mockito.when(mLockHandle.getLockState("ks1" + "." + "tn1" + "." + "pk1"))
                        .thenReturn(musicLockState);
        Mockito.when(mDstoreHandle.executeCriticalGet(preparedQueryObject)).thenReturn(rs);
        ResultSet rs1 = MusicCore.atomicGet("ks1", "tn1", "pk1", preparedQueryObject);
        assertNotNull(rs1);
        Mockito.verify(mLockHandle).createLockId("/" + "ks1.tn1.pk1");
        Mockito.verify(mLockHandle, Mockito.atLeastOnce()).getLockState("ks1.tn1.pk1");
        Mockito.verify(mLockHandle).isMyTurn("id1");
        Mockito.verify(mLockHandle, Mockito.atLeastOnce())
                        .getLockState("ks1" + "." + "tn1" + "." + "pk1");
        Mockito.verify(mDstoreHandle).executeCriticalGet(preparedQueryObject);
    }

    @Test
    public void testAtomicGetPreparedQuerywithAcquireLockWithLeaseFalse()
                    throws MusicServiceException, MusicLockingException {
        mDstoreHandle = Mockito.mock(MusicDataStore.class);
        preparedQueryObject = Mockito.mock(PreparedQueryObject.class);
        rs = Mockito.mock(ResultSet.class);
        Mockito.when(mLockHandle.createLockId("/" + "ks1.tn1.pk1")).thenReturn("id1");
        Mockito.when(mLockHandle.isMyTurn("id1")).thenReturn(false);
        ResultSet rs1 = MusicCore.atomicGet("ks1", "tn1", "pk1", preparedQueryObject);
        assertNull(rs1);
        Mockito.verify(mLockHandle).createLockId("/" + "ks1.tn1.pk1");
        Mockito.verify(mLockHandle).isMyTurn("id1");
    }

    @Test
    public void testGetPreparedQuery() throws MusicServiceException, MusicQueryException {
        mDstoreHandle = Mockito.mock(MusicDataStore.class);
        rs = Mockito.mock(ResultSet.class);
        preparedQueryObject = Mockito.mock(PreparedQueryObject.class);
        session = Mockito.mock(Session.class);
        Mockito.when(mDstoreHandle.getSession()).thenReturn(session);
        Mockito.when(mDstoreHandle.executeEventualGet(preparedQueryObject)).thenReturn(rs);
        ResultSet rs1 = MusicCore.get(preparedQueryObject);
        assertNotNull(rs1);
        Mockito.verify(mDstoreHandle).executeEventualGet(preparedQueryObject);

    }

    @Test
    public void testcriticalGetPreparedQuery() throws MusicServiceException, MusicQueryException, MusicLockingException {
        mDstoreHandle = Mockito.mock(MusicDataStore.class);
        preparedQueryObject = Mockito.mock(PreparedQueryObject.class);
        MusicLockState musicLockState = new MusicLockState(LockStatus.UNLOCKED, "id1");
        rs = Mockito.mock(ResultSet.class);
        session = Mockito.mock(Session.class);
        Mockito.when(mDstoreHandle.getSession()).thenReturn(session);
        Mockito.when(mLockHandle.getLockState("ks1" + "." + "tn1" + "." + "pk1"))
                        .thenReturn(musicLockState);
        Mockito.when(mDstoreHandle.executeCriticalGet(preparedQueryObject)).thenReturn(rs);
        ResultSet rs1 = MusicCore.criticalGet("ks1", "tn1", "pk1", preparedQueryObject, "id1");
        assertNotNull(rs1);
        Mockito.verify(mLockHandle, Mockito.atLeastOnce())
                        .getLockState("ks1" + "." + "tn1" + "." + "pk1");
        Mockito.verify(mDstoreHandle).executeCriticalGet(preparedQueryObject);
    }

    @Test
    public void testcriticalGetPreparedQuerywithInvalidLockId() throws MusicServiceException, MusicLockingException {
        mDstoreHandle = Mockito.mock(MusicDataStore.class);
        preparedQueryObject = Mockito.mock(PreparedQueryObject.class);
        MusicLockState musicLockState = new MusicLockState(LockStatus.UNLOCKED, "id2");
        Mockito.when(mLockHandle.getLockState("ks1" + "." + "tn1" + "." + "pk1"))
                        .thenReturn(musicLockState);
        ResultSet rs1 = MusicCore.criticalGet("ks1", "tn1", "pk1", preparedQueryObject, "id1");
        assertNull(rs1);
        Mockito.verify(mLockHandle, Mockito.atLeastOnce())
                        .getLockState("ks1" + "." + "tn1" + "." + "pk1");
    }

}
