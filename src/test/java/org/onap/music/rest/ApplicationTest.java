/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2019 AT&T Intellectual Property
 *  
 *  Modifications Copyright (C) 2019 IBM.
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
package org.onap.music.rest;

import static org.junit.Assert.*;

import org.junit.Test;

/**
 * @author SaiLakshmiSailakshmi
 *
 */
public class ApplicationTest {
	
	    private String application_name="Application";
	    private String username="music";
	    private String password="music";
	    private String keyspace_name="music";
	    private boolean is_aaf=true;;
	    private String uuid="m12";
	    private boolean is_api=false;
	    
	    Application a=new Application(); 

	@Test
	public void testgetApplication_name() {
		a.getApplication_name();
		}
	
	@Test
	public void testsetApplication_name() {
        a.setApplication_name(application_name);
    }
	
	@Test
    public void testgetUsername() {
        a.getUsername();
    }
	
	@Test
    public void testsetUsername() {
        a.setUsername(username);
    }
    
	@Test
	public void testgetPassword() {
       a.getPassword();
    }
	
	@Test
    public void testsetPassword() {
        a.setPassword(password);
    }
    
	@Test
	public void testgetKeyspace_name() {
        a.getKeyspace_name();
    }
    
	@Test
	public void testsetKeyspace_name() {
        a.setKeyspace_name( keyspace_name);
    }
    
	@Test
	public void testisIs_aaf() {
        a.isIs_aaf();
    }
    
	@Test
	public void testsetIs_aaf() {
    	a.setIs_aaf(is_aaf);
    }
    
	@Test
	public void testgetUuid() {
        a.getUuid();
    }
    
	@Test
	public void testsetUuid() {
        a.setUuid(uuid);
    }
    
	@Test
	public void testgetIs_api() {
        a.getIs_api();
    }
    
	@Test
	public void testsetIs_api() {
        a.setIs_api(is_api);
    }
    
	

}
