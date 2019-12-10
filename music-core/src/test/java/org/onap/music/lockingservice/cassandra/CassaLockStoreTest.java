/*
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
 */

package org.onap.music.lockingservice.cassandra;

import static org.junit.Assert.assertEquals;

import java.util.Iterator;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.onap.music.datastore.MusicDataStore;
import org.onap.music.exceptions.MusicLockingException;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.main.DeadlockDetectionUtil;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.WriteType;
import com.datastax.driver.core.exceptions.WriteTimeoutException;

public class CassaLockStoreTest {

	private CassaLockStore cassaLockStore;
	private CassaLockStore.LockObject lockObject;
	private MusicDataStore dsHandle;

	@Before
	public void setUp() {
		dsHandle = Mockito.mock(MusicDataStore.class);
		cassaLockStore = new CassaLockStore(dsHandle);
		lockObject = cassaLockStore.new LockObject(false, null, null, null, null, null);
	}

	@Test
	public void testLockOwner() {
		lockObject.setIsLockOwner(true);
		assertEquals(true, lockObject.getIsLockOwner());

		lockObject.setIsLockOwner(false);
		assertEquals(false, lockObject.getIsLockOwner());
	}

	@Test
	public void testAcquireTime() {
		lockObject.setAcquireTime("2019-11-11T15:42:12+00:00");
		assertEquals("2019-11-11T15:42:12+00:00", lockObject.getAcquireTime());
	}

	@Test
	public void testCreateTime() {
		lockObject.setCreateTime("2019-11-11T15:43:44+00:00");
		assertEquals("2019-11-11T15:43:44+00:00", lockObject.getCreateTime());
	}

	@Test
	public void testLockRef() {
		lockObject.setLockRef("LockReference");
		assertEquals("LockReference", lockObject.getLockRef());
	}

	@Test
	public void testLockType() {
		lockObject.setLocktype(LockType.READ);
		assertEquals(LockType.READ, lockObject.getLocktype());
	}

	@Test
	public void testOwner() {
		lockObject.setOwner("Owner");
		assertEquals("Owner", lockObject.getOwner());
	}

	@Test
	public void testCreateLockQueue() {
		try {
			Mockito.when(dsHandle.executePut(Mockito.any(), Mockito.any())).thenReturn(true);
			assertEquals(true, cassaLockStore.createLockQueue("keyspace1", "table1"));
		} catch (MusicServiceException | MusicQueryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Test
	public void testGenLockRefandEnQueue() {
		ResultSet resultSetMock = Mockito.mock(ResultSet.class);
		List<Row> latestGuardRow = Mockito.mock(List.class);
		Mockito.when(latestGuardRow.isEmpty()).thenReturn(false);
		Row row = Mockito.mock(Row.class);
		Mockito.when(latestGuardRow.get(0)).thenReturn(row);
		Mockito.when(row.getLong(0)).thenReturn((long) 4);
		Mockito.when(resultSetMock.all()).thenReturn(latestGuardRow);
		try {
			Mockito.when(dsHandle.executeOneConsistencyGet(Mockito.any())).thenReturn(resultSetMock);
			Mockito.when(dsHandle.executePut(Mockito.any(), Mockito.any())).thenReturn(true);
			assertEquals("$keyspace2.table2.lockName2$5",
					cassaLockStore.genLockRefandEnQueue("keyspace2", "table2", "lockName2", LockType.READ, "owner2"));
		} catch (MusicServiceException | MusicQueryException | MusicLockingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Test
	public void testGetLockQueue() {
		ResultSet resultSetMock = Mockito.mock(ResultSet.class);
		Iterator<Row> iterator = Mockito.mock(Iterator.class);
		Mockito.when(iterator.hasNext()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
		Row row = Mockito.mock(Row.class);
		Mockito.when(row.getLong("lockReference")).thenReturn((long)1).thenReturn((long)2).thenReturn((long)3);
		Mockito.when(row.get("lockType", LockType.class)).thenReturn(LockType.WRITE).thenReturn(LockType.WRITE).thenReturn(LockType.WRITE);
		Mockito.when(iterator.next()).thenReturn(row).thenReturn(row).thenReturn(row);
		Mockito.when(resultSetMock.iterator()).thenReturn(iterator);
		
		try {
			Mockito.when(dsHandle.executeOneConsistencyGet(Mockito.any())).thenReturn(resultSetMock);
			assertEquals("2", cassaLockStore.getLockQueue("keyspace2", "table2", "key2").get(1));
		} catch (MusicServiceException | MusicQueryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Test
	public void testGetLockQueueSize() {
		ResultSet resultSetMock = Mockito.mock(ResultSet.class);
		Row row = Mockito.mock(Row.class);
		Mockito.when(resultSetMock.one()).thenReturn(row);
		Mockito.when(row.getLong("count")).thenReturn((long) 6);
		try {
			Mockito.when(dsHandle.executeOneConsistencyGet(Mockito.any())).thenReturn(resultSetMock);
			assertEquals(6, cassaLockStore.getLockQueueSize("keyspace3", "table3", "key3"));
		} catch (MusicServiceException | MusicQueryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Test
	public void testPeekLockQueue() {
		ResultSet resultSetMock = Mockito.mock(ResultSet.class);
		Row row = Mockito.mock(Row.class);
		Mockito.when(row.isNull("lockReference")).thenReturn(false);
		Mockito.when(row.getLong("lockReference")).thenReturn((long) 6);
		Mockito.when(row.getString("createTime")).thenReturn("2019-11-13T15:05:45+00:00");
		Mockito.when(row.getString("acquireTime")).thenReturn("2019-11-13T15:05:45+00:00");
		Mockito.when(row.isNull("lockReference")).thenReturn(false);
		Mockito.when(resultSetMock.one()).thenReturn(row);
		try {
			Mockito.when(dsHandle.executeOneConsistencyGet(Mockito.any())).thenReturn(resultSetMock);
			assertEquals("6", cassaLockStore.peekLockQueue("keyspace4", "table4", "key4").getLockRef());
		} catch (MusicServiceException | MusicQueryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Test
	public void testGetCurrentLockHolders() {
		ResultSet resultSetMock = Mockito.mock(ResultSet.class);
		Iterator<Row> iterator = Mockito.mock(Iterator.class);
		Mockito.when(iterator.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
		Row row = Mockito.mock(Row.class);
		Mockito.when(row.getLong("lockReference")).thenReturn((long) 5).thenReturn((long) 5);
		Mockito.when(row.get("lockType", LockType.class)).thenReturn(LockType.WRITE);
		Mockito.when(iterator.next()).thenReturn(row).thenReturn(row);
		Mockito.when(resultSetMock.iterator()).thenReturn(iterator);
		try {
			Mockito.when(dsHandle.executeOneConsistencyGet(Mockito.any())).thenReturn(resultSetMock);
			assertEquals("$keyspace5.table5.key5$5", cassaLockStore.getCurrentLockHolders("keyspace5", "table5", "key5").get(1));
		} catch (MusicServiceException | MusicQueryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Test
	public void testIsLockOwner() {
		ResultSet resultSetMock = Mockito.mock(ResultSet.class);
		Iterator<Row> iterator = Mockito.mock(Iterator.class);
		Mockito.when(iterator.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
		Row row = Mockito.mock(Row.class);
		Mockito.when(row.getLong("lockReference")).thenReturn((long) 5);
		Mockito.when(row.get("lockType", LockType.class)).thenReturn(LockType.WRITE);
		Mockito.when(iterator.next()).thenReturn(row).thenReturn(row);
		Mockito.when(resultSetMock.iterator()).thenReturn(iterator);
		try {
			Mockito.when(dsHandle.executeOneConsistencyGet(Mockito.any())).thenReturn(resultSetMock);
			assertEquals(true, cassaLockStore.isLockOwner("keyspace5", "table5", "key5", "5"));
		} catch (MusicServiceException | MusicQueryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Test
	public void testGetLockInfo() {
		ResultSet resultSetMock = Mockito.mock(ResultSet.class);
		Row row = Mockito.mock(Row.class);
		Mockito.when(row.isNull("lockReference")).thenReturn(false);
		Mockito.when(row.getLong("lockReference")).thenReturn((long) 6);
		Mockito.when(row.getString("createTime")).thenReturn("2019-11-13T15:05:45+00:00");
		Mockito.when(row.getString("acquireTime")).thenReturn("2019-11-13T15:05:45+00:00");
		LockType locktype = Mockito.mock(LockType.class);
		Mockito.when(row.get("lockType", LockType.class)).thenReturn(locktype);
		Mockito.when(row.getString("owner")).thenReturn("owner6");
		Mockito.when(resultSetMock.one()).thenReturn(row);

		try {
			Mockito.when(dsHandle.executeOneConsistencyGet(Mockito.any())).thenReturn(resultSetMock);
			CassaLockStore csLockStore = Mockito.spy(cassaLockStore);
			Mockito.doReturn(true).when(csLockStore).isLockOwner(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
			assertEquals("6", csLockStore.getLockInfo("keyspace6", "table6", "key6", "6").getLockRef());
		} catch (MusicServiceException | MusicQueryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Test
	public void testCheckForDeadlock() {
		DeadlockDetectionUtil ddu = Mockito.mock(DeadlockDetectionUtil.class);
		ResultSet resultSetMock = Mockito.mock(ResultSet.class);
		Iterator<Row> it = Mockito.mock(Iterator.class);
		Row row = Mockito.mock(Row.class);
		Mockito.when(it.hasNext()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
		Mockito.when(row.getString("key")).thenReturn("key8");
		Mockito.when(row.getString("owner")).thenReturn("owner8");
		Mockito.when(row.getString("acquiretime")).thenReturn("1");
		Mockito.when(it.next()).thenReturn(row).thenReturn(row).thenReturn(row);
		Mockito.when(resultSetMock.iterator()).thenReturn(it);
		CassaLockStore csLockStore = Mockito.spy(cassaLockStore);
		Mockito.doReturn(ddu).when(csLockStore).getDeadlockDetectionUtil();
		Mockito.when(ddu.checkForDeadlock(Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(true);
		try {
			Mockito.when(dsHandle.executeLocalQuorumConsistencyGet(Mockito.any())).thenReturn(resultSetMock);
			assertEquals(false,
					cassaLockStore.checkForDeadlock("keyspace8", "table8", "lockName8", LockType.WRITE, "owner8", true));
		} catch (MusicServiceException | MusicQueryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Test
	public void testGetAllLocksForOwner() {
		ResultSet resultSetMock = Mockito.mock(ResultSet.class);
		Iterator<Row> it = Mockito.mock(Iterator.class);
		Mockito.when(it.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
		Row row = Mockito.mock(Row.class);
		Mockito.when(row.getString("key")).thenReturn("key10");
		Mockito.when(row.getLong("lockreference")).thenReturn((long) 10);
		Mockito.when(it.next()).thenReturn(row);
		Mockito.when(resultSetMock.iterator()).thenReturn(it);
		try {
			Mockito.when(dsHandle.executeQuorumConsistencyGet(Mockito.any())).thenReturn(resultSetMock);
			assertEquals("key10$10", cassaLockStore.getAllLocksForOwner("owneer10", "keyspace10", "table10").get(1));
		} catch (MusicServiceException | MusicQueryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Test
    public void testDequeueLockRef() throws Exception {
        cassaLockStore.deQueueLockRef("keyspace1", "table1", "key6", "6", 2);
        
        // note only expecting 1 call to this instance, expecting it to succeed
        Mockito.verify(dsHandle, Mockito.times(1)).executePut(Mockito.any(), Mockito.anyString());
    }
	
	@Test
	public void testDequeueLockRefWriteTimeout() throws Exception {
	    int retryCount = 22;
	    try {
	        Mockito.when(dsHandle.executePut(Mockito.any(), Mockito.anyString()))
                .thenThrow(new MusicServiceException("Cassandra timeout during..."));
            cassaLockStore.deQueueLockRef("keyspace1", "table1", "key6", "6", retryCount);
            
            // Should never reach here
            assertEquals(false, true);
        } catch (MusicServiceException | MusicQueryException | MusicLockingException e) {
            // should throw an error
        }
	    
	    Mockito.verify(dsHandle, Mockito.times(retryCount)).executePut(Mockito.any(), Mockito.anyString());
	}
}
