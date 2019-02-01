/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2017 IBM Intellectual Property
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
package org.onap.music.rest.repository.impl;

import java.util.Iterator;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;
import org.onap.music.datastore.jsonobjects.JsonOnboard;
import org.onap.music.main.CachingUtil;
import org.onap.music.rest.repository.RestMusicAdminRepository;
import org.onap.music.rest.service.RestMusicAdminService;
import org.onap.music.rest.service.impl.RestMusicAdminServiceImpl;
import org.powermock.modules.junit4.PowerMockRunner;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import jdk.nashorn.internal.ir.annotations.Ignore;




@RunWith(MockitoJUnitRunner.class)
public class RestMusicAdminServiceImplTest {
	
	@InjectMocks
	RestMusicAdminServiceImpl restMusicAdminServiceImpl;
	
	@Spy
	RestMusicAdminRepository restMusicAdminRepository;
	
	@Mock
	Iterator<Row> it;
	
	@Mock
	Row row;
	
	@Mock
	ResultSet rs;
	JsonOnboard jsonObj=new JsonOnboard();
	
	
	@Before
	public void setup() {
		jsonObj.setAid("AID123");
		jsonObj.setAppname("AppName");
		jsonObj.setIsAAF("false");
		jsonObj.setPassword("1234");
		jsonObj.setUserId("user123");
	}
	
	/*@Test
	@Ignore
	public void createLockReferenceTest() throws Exception {
		Mockito.when(restMusicAdminRepository.getUuidFromKeySpaceMasterUsingAppName("AppName")).thenReturn(rs);
		Mockito.when(restMusicAdminRepository.insertValuesIntoKeySpaceMaster("1234", "AppName", "user123", "false", "1234")).thenReturn("SUCCESS");
		restMusicAdminServiceImpl.onboardAppWithMusic(jsonObj);
	}*/
	
	@Test
	public void getOnboardedInfoSearchTest() throws Exception {
		Mockito.when(rs.iterator()).thenReturn(it);
		Mockito.when(restMusicAdminRepository.fetchOnboardedInfoSearch("AppName", "AID123", "false")).thenReturn(rs);
		Assert.assertNotNull(restMusicAdminServiceImpl.getOnboardedInfoSearch(jsonObj));
	}
	
	@Test
	public void deleteOnboardAppTest() throws Exception {
		Mockito.when(row.getString("keyspace_name")).thenReturn("TBD");
		Mockito.when(rs.one()).thenReturn(row);
		Mockito.when(restMusicAdminRepository.getKeySpaceNameFromKeySpaceMasterWithUuid("AID123")).thenReturn(rs);
		Assert.assertNotNull(restMusicAdminServiceImpl.deleteOnboardApp(jsonObj));
	}
}
