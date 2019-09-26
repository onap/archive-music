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

public class ApplicationTest {

	Application apl=new Application();
	 private String application_name="music";
	 private String username="music";
	 private String password="music";
	 private String keyspace_name="music";
	 private boolean is_aaf=false;
	 private String uuid="123";
	 private boolean is_api=true;
	
	@Test
	public void testsetApplication_name() {
		apl.setApplication_name(application_name);
		assertEquals("music",apl.getApplication_name());
	}
	
	@Test
	public void testsetUsername()
	{
		apl.setUsername(username);
		assertEquals("music",apl.getUsername());
	}
	
	@Test
	public void testsetPassword()
	{
		apl.setPassword(password);
		assertEquals("music",apl.getPassword());
	}
	
	@Test
	public void testsetKeyspace_name()
	{
		apl.setKeyspace_name(keyspace_name);
		assertEquals("music",apl.getKeyspace_name());
	}
	
	@Test
	public void testsetIs_aaf()
	{
		apl.setIs_aaf(is_aaf);
		assertEquals(false,apl.isIs_aaf());
	}
	
	
	@Test
	public void testsetUuid()
	{
		apl.setUuid(uuid);
		assertEquals("123",apl.getUuid());
	}
	
	@Test
	public void testsetIs_api()
	{
		apl.setIs_api(is_api);
		assertEquals(true,apl.getIs_api());
		
	}
	
	
  
}
