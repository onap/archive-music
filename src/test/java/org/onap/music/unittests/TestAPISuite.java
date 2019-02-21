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

package org.onap.music.unittests;

import java.util.List;
import java.util.UUID;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;
import org.mindrot.jbcrypt.BCrypt;
import org.onap.music.datastore.MusicDataStoreHandle;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.lockingservice.cassandra.CassaLockStore;
import org.onap.music.main.MusicCore;
import org.onap.music.main.MusicUtil;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.sun.jersey.core.util.Base64;

@RunWith(Suite.class)
@SuiteClasses({ TstRestMusicDataAPI.class, TstRestMusicLockAPI.class, TstRestMusicAdminAPI.class,
    TstRestMusicConditionalAPI.class})
public class TestAPISuite {

	static String appName = "TestApp";
	static String userId = "TestUser";
	static String password = "TestPassword";
	static String authData = userId+":"+password;
	static String wrongAuthData = userId+":"+"pass";
	static String authorization = new String(Base64.encode(authData.getBytes()));
	static String wrongAuthorization = new String(Base64.encode(wrongAuthData.getBytes()));
	static boolean isAAF = false;
	static UUID uuid = UUID.fromString("abc66ccc-d857-4e90-b1e5-df98a3d40ce6");
	static String keyspaceName = "testcassa";
	static String tableName = "employees";
	static String xLatestVersion = "X-latestVersion";
	static String onboardUUID = null;
	
	@BeforeClass
	public static void beforeClass() throws Exception {
		MusicDataStoreHandle.mDstoreHandle = CassandraCQL.connectToEmbeddedCassandra();
		MusicCore.mLockHandle = new CassaLockStore(MusicDataStoreHandle.mDstoreHandle);
		createAdminTable();
	}
	
	@AfterClass
	public static void afterClass() {
		PreparedQueryObject testObject = new PreparedQueryObject();
		testObject.appendQueryString("DROP KEYSPACE IF EXISTS admin");
		MusicCore.eventualPut(testObject);
		if(MusicDataStoreHandle.mDstoreHandle!=null)
			MusicDataStoreHandle.mDstoreHandle.close();
	}
	
	private static void createAdminTable() throws Exception {
		PreparedQueryObject testObject = new PreparedQueryObject();
		testObject.appendQueryString(CassandraCQL.createAdminKeyspace);
		MusicCore.eventualPut(testObject);
		testObject = new PreparedQueryObject();
		testObject.appendQueryString(CassandraCQL.createAdminTable);
		MusicCore.eventualPut(testObject);

		testObject = new PreparedQueryObject();
		testObject.appendQueryString(
				"INSERT INTO admin.keyspace_master (uuid, keyspace_name, application_name, is_api, "
						+ "password, username, is_aaf) VALUES (?,?,?,?,?,?,?)");
		testObject.addValue(MusicUtil.convertToActualDataType(DataType.uuid(), uuid));
		testObject.addValue(MusicUtil.convertToActualDataType(DataType.text(),
				MusicUtil.DEFAULTKEYSPACENAME));
		testObject.addValue(MusicUtil.convertToActualDataType(DataType.text(), appName));
		testObject.addValue(MusicUtil.convertToActualDataType(DataType.cboolean(), "True"));
		testObject.addValue(MusicUtil.convertToActualDataType(DataType.text(), BCrypt.hashpw(password, BCrypt.gensalt())));
		testObject.addValue(MusicUtil.convertToActualDataType(DataType.text(), userId));
		testObject.addValue(MusicUtil.convertToActualDataType(DataType.cboolean(), isAAF));
		MusicCore.eventualPut(testObject);

		testObject = new PreparedQueryObject();
		testObject.appendQueryString(
				"select uuid from admin.keyspace_master where application_name = ? allow filtering");
		testObject.addValue(MusicUtil.convertToActualDataType(DataType.text(), appName));
		ResultSet rs = MusicCore.get(testObject);
		List<Row> rows = rs.all();
		if (rows.size() > 0) {
			System.out.println("#######UUID is:" + rows.get(0).getUUID("uuid"));
		}
	}
}
