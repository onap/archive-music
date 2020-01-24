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

package org.onap.music.main;

import static org.junit.Assert.assertEquals;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.MultivaluedMap;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.FieldSetter;
import org.onap.music.datastore.Condition;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.datastore.jsonobjects.JsonDelete;
import org.onap.music.datastore.jsonobjects.JsonIndex;
import org.onap.music.datastore.jsonobjects.JsonInsert;
import org.onap.music.datastore.jsonobjects.JsonKeySpace;
import org.onap.music.datastore.jsonobjects.JsonSelect;
import org.onap.music.datastore.jsonobjects.JsonTable;
import org.onap.music.datastore.jsonobjects.JsonUpdate;
import org.onap.music.exceptions.MusicLockingException;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.lockingservice.cassandra.CassaLockStore;
import org.onap.music.lockingservice.cassandra.LockType;
import org.onap.music.lockingservice.cassandra.MusicLockState;
import org.onap.music.service.MusicCoreService;
import com.datastax.driver.core.ResultSet;

public class MusicCoreTest {

    MusicCore mCore;
    MusicCoreService musicCore;
    CassaLockStore mLockHandle;
    
    @Before
    public void setup() {
        mCore = new MusicCore();
        musicCore = Mockito.mock(MusicCoreService.class);
        mLockHandle = Mockito.mock(CassaLockStore.class);
        try {
            FieldSetter.setField(mCore, mCore.getClass().getDeclaredField("musicCore"), musicCore);
        } catch (NoSuchFieldException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (SecurityException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    @Test
    public void testAcquireLock() {
        ReturnType returnType = Mockito.mock(ReturnType.class);
        ReturnType result = null;
        try {
            Mockito.when(musicCore.acquireLock(Mockito.any(), Mockito.any())).thenReturn(returnType);
            result = MusicCore.acquireLock("key1", "lockid1");
        } catch (MusicLockingException | MusicQueryException | MusicServiceException e) {

        }
        assertEquals(returnType, result);
    }
    
    @Test
    public void testacquireLockWithLease() {
        ReturnType returnType = Mockito.mock(ReturnType.class);
        ReturnType result = null;
        try {
            Mockito.when(musicCore.acquireLockWithLease(Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(returnType);
            result = MusicCore.acquireLockWithLease("key1", "lockid1", 100L);
        } catch (MusicLockingException | MusicQueryException | MusicServiceException | SecurityException e) {

        }
        assertEquals(returnType, result);
    }
    
    @Test
    public void testCreateLockReferenceAtomic() {
        String result = null;
        try {
            Mockito.when(musicCore.createLockReferenceAtomic(Mockito.any())).thenReturn("lockreference1");
            result = MusicCore.createLockReferenceAtomic("key2");
        } catch (MusicLockingException e) {

        }
        assertEquals("lockreference1", result);
    }
    
    @Test
    public void testCreateLockReference() {
        String result = null;
        try {
            Mockito.when(musicCore.createLockReference(Mockito.any(), Mockito.any())).thenReturn("lockreference2");
            result = MusicCore.createLockReference("key3", "owner3");
        } catch (MusicLockingException e) {

        }
        assertEquals("lockreference2", result);
    }
    
    @Test
    public void testCreateLockReferenceAtomic2() {
        String result = null;
        try {
            Mockito.when(musicCore.createLockReferenceAtomic(Mockito.any(), Mockito.any())).thenReturn("lockreference3");
            result = MusicCore.createLockReferenceAtomic("key4", LockType.READ);
        } catch (MusicLockingException e) {

        }
        assertEquals("lockreference3", result);
    }
    
    @Test
    public void testCreateLockReference2() {
        String result = null;
        try {
            Mockito.when(musicCore.createLockReference(Mockito.any(), Mockito.any(), Mockito.any())).thenReturn("lockreference4");
            result = MusicCore.createLockReference("key4", LockType.READ, "owner4");
        } catch (MusicLockingException e) {

        }
        assertEquals("lockreference4", result);
    }
    
    @Test
    public void testCreateTable() {
        ResultType resultType = Mockito.mock(ResultType.class);
        ResultType result = null;
        try {
            Mockito.when(musicCore.createTable(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(resultType);
            result = MusicCore.createTable("keyspace1", "table1", new PreparedQueryObject(), "consistency");
        } catch (MusicServiceException e) {

        }
        assertEquals(resultType, result);
    }
    
    @Test
    public void testQuorumGet() {
        ResultSet rs = Mockito.mock(ResultSet.class);
        Mockito.when(musicCore.quorumGet(Mockito.any())).thenReturn(rs);
        assertEquals(rs, MusicCore.quorumGet(new PreparedQueryObject()));
    }
    
    @Test
    public void testWhoseTurnIsIt() {
        Mockito.when(musicCore.whoseTurnIsIt(Mockito.any())).thenReturn("turn");
        assertEquals("turn", MusicCore.whoseTurnIsIt("key5"));
    }
    
    @Test
    public void testGetCurrentLockHolders() {
        List<String> result = Mockito.mock(List.class);
        Mockito.when(musicCore.getCurrentLockHolders(Mockito.any())).thenReturn(result);
        assertEquals(result, MusicCore.getCurrentLockHolders("key6"));
    }
    
    @Test
    public void testPromoteLock() {
        ReturnType returnType = Mockito.mock(ReturnType.class);
        ReturnType result = null;
        try {
            Mockito.when(musicCore.promoteLock(Mockito.any())).thenReturn(returnType);
            result = MusicCore.promoteLock("lockid2");
        } catch (MusicLockingException e) {

        }
        assertEquals(returnType, result);
    }
    
    @Test
    public void testEventualPut() {
        ReturnType returnType = Mockito.mock(ReturnType.class);
        Mockito.when(musicCore.eventualPut(Mockito.any())).thenReturn(returnType);
        assertEquals(returnType, MusicCore.eventualPut(new PreparedQueryObject()));
    }
    
    @Test
    public void testEventualPut_nb() {
        ReturnType returnType = Mockito.mock(ReturnType.class);
        Mockito.when(musicCore.eventualPut_nb(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(returnType);
        assertEquals(returnType, MusicCore.eventualPut_nb(new PreparedQueryObject(), "keyspace2", "table2", "primarykey1"));
    }
    
    @Test
    public void testCriticalPut() {
        ReturnType returnType = Mockito.mock(ReturnType.class);
        Mockito.when(musicCore.criticalPut(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), 
                Mockito.any(), Mockito.any())).thenReturn(returnType);
        assertEquals(returnType, MusicCore.criticalPut("keyspace3", "table3", "primarykey2", new PreparedQueryObject(), "lockreference2", new Condition(new HashMap(), new PreparedQueryObject())));
    }
    
    @Test
    public void testNonKeyRelatedPut() {
        ResultType resultType = Mockito.mock(ResultType.class);
        ResultType result = null;
        try {
            Mockito.when(musicCore.nonKeyRelatedPut(Mockito.any(), Mockito.any())).thenReturn(resultType);
            result = MusicCore.nonKeyRelatedPut(new PreparedQueryObject(), "consistency2");
        } catch (MusicServiceException | MusicQueryException e) {

        }
        assertEquals(resultType, result);
    }
    
    @Test
    public void testGet() {
        ResultSet rs = Mockito.mock(ResultSet.class);
        ResultSet result = null;
        try {
            Mockito.when(musicCore.get(Mockito.any())).thenReturn(rs);
            result = MusicCore.get(new PreparedQueryObject());
        } catch (MusicServiceException e) {

        }
        assertEquals(rs, result);
    }
    
    @Test
    public void testCriticalGet() {
        ResultSet rs = Mockito.mock(ResultSet.class);
        ResultSet result = null;
        try {
            Mockito.when(musicCore.criticalGet(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), 
                    Mockito.any())).thenReturn(rs);
            result = MusicCore.criticalGet("keyspace4", "table4", "primarykey3", new PreparedQueryObject(), "lockreference3");
        } catch (MusicServiceException e) {

        }
        assertEquals(rs, result);
    }
    
    @Test
    public void testAtomicPut() {
        ReturnType returnType = Mockito.mock(ReturnType.class);
        ReturnType result = null;
        try {
            Mockito.when(musicCore.atomicPut(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), 
                    Mockito.any())).thenReturn(returnType);
            result = MusicCore.atomicPut("keyspace5", "table5", "primarykey4", new PreparedQueryObject(), 
                    new Condition(new HashMap(), new PreparedQueryObject()));
        } catch (MusicLockingException | MusicQueryException | MusicServiceException e) {

        }
        assertEquals(returnType, result);
    }
    
    @Test
    public void testAtomicGet() {
        ResultSet rs = Mockito.mock(ResultSet.class);
        ResultSet result = null;
        try {
            Mockito.when(musicCore.atomicGet(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(rs);
            result = MusicCore.atomicGet("keyspace5", "table5", "primarykey4", new PreparedQueryObject());
        } catch (MusicServiceException | MusicLockingException | MusicQueryException e) {

        }
        assertEquals(rs, result);
    }
    
    @Test
    public void testGetLockQueue() {
        List<String> result = Mockito.mock(List.class);
        List<String> rst = null;
        try {
            Mockito.when(musicCore.getLockQueue(Mockito.any())).thenReturn(result);
            rst = MusicCore.getLockQueue("key5");
        } catch (MusicServiceException | MusicQueryException | MusicLockingException e) {

        }
        assertEquals(result, rst);
    }
    
    @Test
    public void testGetLockQueueSize() {
        long result = 0L;
        try {
            Mockito.when(musicCore.getLockQueueSize(Mockito.any())).thenReturn(100L);
            result = MusicCore.getLockQueueSize("key6");
        } catch (MusicServiceException | MusicQueryException | MusicLockingException e) {

        }
        assertEquals(100L, result);
    }
    
    @Test
    public void testatomicPutWithDeleteLock() {
        ReturnType returnType = Mockito.mock(ReturnType.class);
        ReturnType result = null;
        try {
            Mockito.when(musicCore.atomicPutWithDeleteLock(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), 
                    Mockito.any())).thenReturn(returnType);
            result =  MusicCore.atomicPutWithDeleteLock("keyspace5", "table5", "primarykey4", new PreparedQueryObject(), 
                    new Condition(new HashMap(), new PreparedQueryObject()));
        } catch (MusicLockingException e) {

        }
        assertEquals(returnType, result);
    }
    
    @Test
    public void testAtomicGetWithDeleteLock() {
        ResultSet rs = Mockito.mock(ResultSet.class);
        ResultSet result = null;
        try {
            Mockito.when(musicCore.atomicGetWithDeleteLock(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(rs);
            result = MusicCore.atomicGetWithDeleteLock("keyspace5", "table5", "primarykey4", new PreparedQueryObject());
        } catch (MusicServiceException | MusicLockingException e) {

        }
        assertEquals(rs, result);
    }
    
    @Test
    public void testValidateLock() {
        Map<String, Object> map = Mockito.mock(Map.class);
        Mockito.when(musicCore.validateLock(Mockito.any())).thenReturn(map);
        assertEquals(map, MusicCore.validateLock("lockname"));
    }
    
    @Test
    public void testReleaseLock() {
        MusicLockState musicLockState = Mockito.mock(MusicLockState.class);
        MusicLockState result = null;
        try {
            Mockito.when(musicCore.releaseLock(Mockito.any(), Mockito.any())).thenReturn(musicLockState);
            result = MusicCore.releaseLock("lockid", true);
        } catch (MusicLockingException e) {

        }
        assertEquals(musicLockState, result);
    }
    
    @Test
    public void testReleaseAllLocksForOwner() {
        List<String> result = Mockito.mock(List.class);
        List<String> rst = null;
        try {
            Mockito.when(musicCore.releaseAllLocksForOwner(Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(result);
            rst = MusicCore.releaseAllLocksForOwner("owner2", "keyspace6", "table6");
        } catch (MusicServiceException | MusicQueryException | MusicLockingException e) {

        }
        assertEquals(result, rst);
    }
    
    @Test
    public void testCreateKeyspace() {
        ResultType resultType = Mockito.mock(ResultType.class);
        ResultType result = null;
        try {
            Mockito.when(musicCore.createKeyspace(Mockito.any(), Mockito.any())).thenReturn(resultType);
            result = MusicCore.createKeyspace(new JsonKeySpace(), "consistency3");
        } catch (MusicServiceException | MusicQueryException e) {

        }
        assertEquals(resultType, result);
    }
    
    @Test
    public void testDropKeyspace() {
        ResultType resultType = Mockito.mock(ResultType.class);
        ResultType result = null;
        try {
            Mockito.when(musicCore.dropKeyspace(Mockito.any(), Mockito.any())).thenReturn(resultType);
            result = MusicCore.dropKeyspace(new JsonKeySpace(), "consistency4");
        } catch (MusicServiceException | MusicQueryException e) {

        }
        assertEquals(resultType, result);
    }
    
    @Test
    public void testCreateTable2() {
        ResultType resultType = Mockito.mock(ResultType.class);
        ResultType result = null;
        try {
            Mockito.when(musicCore.createTable(Mockito.any(), Mockito.any())).thenReturn(resultType);
            result = MusicCore.createTable(new JsonTable(), "consistency5");
        } catch (MusicServiceException | MusicQueryException e) {

        }
        assertEquals(resultType, result);
    }
    
    @Test
    public void testDropTable() {
        ResultType resultType = Mockito.mock(ResultType.class);
        ResultType result = null;
        try {
            Mockito.when(musicCore.dropTable(Mockito.any(), Mockito.any())).thenReturn(resultType);
            result = MusicCore.dropTable(new JsonTable(), "consistency5");
        } catch (MusicServiceException | MusicQueryException e) {

        }
        assertEquals(resultType, result);
    }
    
    @Test
    public void testCreateIndex() {
        ResultType resultType = Mockito.mock(ResultType.class);
        ResultType result = null;
        try {
            Mockito.when(musicCore.createIndex(Mockito.any(), Mockito.any())).thenReturn(resultType);
            result = MusicCore.createIndex(new JsonIndex("indexName", "keyspace7", "table7", "field"), "consistency6");
        } catch (MusicServiceException | MusicQueryException e) {

        }
        assertEquals(resultType, result);
    }
    
    @Test
    public void testSelect() {
        ResultSet rs = Mockito.mock(ResultSet.class);
        ResultSet result = null;
        try {
            Mockito.when(musicCore.select(Mockito.any(), Mockito.any())).thenReturn(rs);
            MultivaluedMap<String, String> map = Mockito.mock(MultivaluedMap.class);
            result = MusicCore.select(new JsonSelect(), map);
        } catch (MusicServiceException | MusicQueryException e) {

        }
        assertEquals(rs, result);
    }
    
    @Test
    public void testSelectCritical() {
        ResultSet rs = Mockito.mock(ResultSet.class);
        ResultSet result = null;
        try {
            Mockito.when(musicCore.selectCritical(Mockito.any(), Mockito.any())).thenReturn(rs);
            MultivaluedMap<String, String> map = Mockito.mock(MultivaluedMap.class);
            result = MusicCore.selectCritical(new JsonInsert(), map);
        } catch (MusicServiceException | MusicLockingException | MusicQueryException e) {

        }
        assertEquals(rs, result);
    }
    
    @Test
    public void testInsertIntoTable() {
        ReturnType returnType = Mockito.mock(ReturnType.class);
        ReturnType result = null;
        try {
            Mockito.when(musicCore.insertIntoTable(Mockito.any())).thenReturn(returnType);
            result = MusicCore.insertIntoTable(new JsonInsert());
        } catch (MusicLockingException | MusicQueryException | MusicServiceException e) {

        }
        assertEquals(returnType, result);
    }
    
    @Test
    public void testUpdateTable() {
        ReturnType returnType = Mockito.mock(ReturnType.class);
        ReturnType result = null;
        try {
            Mockito.when(musicCore.updateTable(Mockito.any(), Mockito.any())).thenReturn(returnType);
            MultivaluedMap<String, String> map = Mockito.mock(MultivaluedMap.class);
            result = MusicCore.updateTable(new JsonUpdate(), map);
        } catch (MusicLockingException | MusicQueryException | MusicServiceException e) {

        }
        assertEquals(returnType, result);
    }
    
    @Test
    public void testDeleteFromTable() {
        ReturnType returnType = Mockito.mock(ReturnType.class);
        ReturnType result = null;
        try {
            MultivaluedMap<String, String> map = Mockito.mock(MultivaluedMap.class);
            Mockito.when(musicCore.deleteFromTable(Mockito.any(), Mockito.any())).thenReturn(returnType);
            result = MusicCore.deleteFromTable(new JsonDelete(), map);
        } catch (MusicLockingException | MusicQueryException | MusicServiceException e) {

        }
        assertEquals(returnType, result);
    }
}
