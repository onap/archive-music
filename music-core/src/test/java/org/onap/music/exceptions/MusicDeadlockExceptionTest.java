/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 * Copyright (c) 2019 IBM Intellectual Property
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

package org.onap.music.exceptions;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class MusicDeadlockExceptionTest {
	
	private String owner = "tester";
	private String keyspace = "testing";
	private String table = "lockq";
	private String key = "test";

	 @Test
	    public void TestException1() {
	        String s1 = "Value1";
	        String s2 = "value2";
	        try {
	            if (!s1.equalsIgnoreCase(s2)) {
	                throw new MusicDeadlockException();
	            }
	        } catch (MusicDeadlockException mde) {
	            assertEquals("org.onap.music.exceptions.MusicDeadlockException", mde.getClass().getName());
	        }

	    }
	 
	 @Test
	    public void TestException6() {
	        String s1 = "Value1";
	        String s2 = "value2";
	        try {
	            if (!s1.equalsIgnoreCase(s2)) {
	                throw new MusicDeadlockException("org.onap.music.exceptions.MusicDeadlockException");
	            }
	        } catch (MusicDeadlockException mde) {
	            assertEquals(mde.getMessage(),"org.onap.music.exceptions.MusicDeadlockException");
	        }

	    }

	    @Test
	    public void TestException2() {
	        String s1 = "Value1";
	        String s2 = "value2";
	        try {
	            if (!s1.equalsIgnoreCase(s2)) {
	                throw new MusicDeadlockException("MusicDeadlockException Exception occured..");
	            }
	        } catch (MusicDeadlockException mde) {
	            assertEquals(mde.getMessage(), "MusicDeadlockException Exception occured..");
	        }

	    }

	    @Test
	    public void TestException3() {
	        String s1 = "Value1";
	        String s2 = "value2";
	        try {
	            if (!s1.equalsIgnoreCase(s2)) {
	                throw new MusicDeadlockException(new Throwable());
	            }
	        } catch (MusicDeadlockException mve) {
	            assertEquals("org.onap.music.exceptions.MusicDeadlockException", mve.getClass().getName());
	        }

	    }

	    @Test
	    public void TestException4() {
	        String message = "Exception occured";
	        String s1 = "Value1";
	        String s2 = "value2";
	        try {
	            if (!s1.equalsIgnoreCase(s2)) {
	                throw new MusicDeadlockException(message, new Throwable());
	            }
	        } catch (MusicDeadlockException mde) {
	            assertEquals("org.onap.music.exceptions.MusicDeadlockException", mde.getClass().getName());
	        }

	    }

	    @Test
	    public void TestException5() {
	        String message = "Exception occured";
	        boolean enableSuppression = true;
	        boolean writableStackTrace = false;
	        String s1 = "Value1";
	        String s2 = "value2";
	        try {
	            if (!s1.equalsIgnoreCase(s2)) {
	                throw new MusicDeadlockException(message, new Throwable(), enableSuppression,
	                        writableStackTrace);
	            }
	        } catch (MusicDeadlockException mde) {
	            assertEquals("org.onap.music.exceptions.MusicDeadlockException", mde.getClass().getName());
	        }

	    }
	    
	    @Test
	    public void TestSetValues()
	    {
	    	MusicDeadlockException mde=new MusicDeadlockException();
	    	mde.setValues(owner,keyspace,table,key);
	    	assertEquals("tester",mde.getOwner());
	    	assertEquals("testing",mde.getKeyspace());
	    	assertEquals("lockq",mde.getTable());
	    	assertEquals("test",mde.getKey());
	    }
}
