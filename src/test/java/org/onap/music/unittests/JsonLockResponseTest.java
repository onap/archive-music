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
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.onap.music.lockingservice.MusicLockState;
import org.onap.music.lockingservice.MusicLockState.LockStatus;
import org.onap.music.main.ResultType;
import org.onap.music.response.jsonobjects.JsonLockResponse;

public class JsonLockResponseTest {
    
    JsonLockResponse response = new JsonLockResponse(ResultType.SUCCESS);
    
    @Test
    public void error1() {
        Boolean result = false;
        response.setError("");
        result = response.getError().equals("error1");
        assertFalse("Success",result);
    }

    @Test
    public void error() {
        Boolean result = false;
        response.setError("error1");
        result = response.getError().equals("error1");
        assertTrue("Success",result);
    }

    @Test
    public void lock1() {
        Boolean result = false;
        response.setLock("");
        result = response.getLock().equals("lock1");
        assertFalse("Success",result);
    }    
    
    @Test
    public void lock() {
        Boolean result = false;
        response.setLock("lock1");
        result = response.getLock().equals("lock1");
        assertTrue("Success",result);
    }

    @Test
    public void status1() {
        Boolean result = false;
        response.setStatus(null);
        result = response.getStatus() == ResultType.SUCCESS;
        assertFalse("Success",result);
    }    
    
    @Test
    public void status() {
        Boolean result = false;
        response.setStatus(ResultType.SUCCESS);
        result = response.getStatus() == ResultType.SUCCESS;
        assertTrue("Success",result);
    }

    
    
    @Test
    public void lockHolder1() {
        Boolean result = false;
        response.setLockHolder("");
        result = response.getLockHolder().equals("LockHolder");
        assertFalse("Success",result);
    }
    
    @Test
    public void lockHolder() {
        Boolean result = false;
        response.setLockHolder("LockHolder");
        result = response.getLockHolder().equals("LockHolder");
        assertTrue("Success",result);
    }

    @Test
    public void lockLease1() {
        Boolean result = false;
        response.setLockLease("");
        result = response.getLockLease().equals("lockLease");
        assertFalse("Success",result);
    }
    
    @Test
    public void lockLease() {
        Boolean result = false;
        response.setLockLease("lockLease");
        result = response.getLockLease().equals("lockLease");
        assertTrue("Success",result);
    }

    @Test
    public void lockStatus1() {
        Boolean result = false;
        response.setLockStatus(null);
        result = response.getLockStatus() == MusicLockState.LockStatus.LOCKED;
        assertFalse("Success",result);
    }

    @Test
    public void lockStatus() {
        Boolean result = false;
        response.setLockStatus(MusicLockState.LockStatus.LOCKED);
        result = response.getLockStatus() == MusicLockState.LockStatus.LOCKED;
        assertTrue("Success",result);
    }
    
    @Test
    public void message1() {
        Boolean result = false;
        response.setMessage("");
        result = response.getMessage().equals("message");
        assertFalse("Success",result);
    }

    @Test
    public void message() {
        Boolean result = false;
        response.setMessage("message");
        result = response.getMessage().equals("message");
        assertTrue("Success",result);
    }

    @Test
    public void map() {
        Boolean result = false;
        response.setMessage("message");
        response.setLockStatus(MusicLockState.LockStatus.LOCKED);
        response.setLockHolder("LockHolder");
        response.setLockLease("lockLease");
        response.setStatus(ResultType.SUCCESS);
        response.setLock("lock1");
        response.setError("error1");     
        Map<String,Object> myMap = response.toMap();
        result = myMap.containsKey("status");
        System.out.println(response.toString());
        assertTrue("Success",result);
    }
    
    @Test
    public void map1() {
        Boolean result = false;
        response.setMessage(null);
        response.setLockStatus(null);
        response.setLockHolder(null);
        response.setLockLease(null);
        response.setStatus(null);
        response.setLock(null);
        response.setError(null);     
        Map<String,Object> myMap = response.toMap();
        result = myMap.containsKey("error");
        System.out.println(result);
        assertFalse("Success",result);
    }

    
    
    
}
