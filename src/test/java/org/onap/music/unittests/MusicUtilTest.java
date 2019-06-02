/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2017 AT&T Intellectual Property
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

package org.onap.music.unittests;

import static org.junit.Assert.*;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.Test;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.main.MusicUtil;
import org.onap.music.main.PropertiesLoader;
import org.springframework.test.context.TestPropertySource;
import com.datastax.driver.core.DataType;

public class MusicUtilTest {

    @Test
    public void testGetCassName() {
        MusicUtil.setCassName("Cassandra");
        assertTrue(MusicUtil.getCassName().equals("Cassandra"));
    }

    @Test
    public void testGetCassPwd() {
        MusicUtil.setCassPwd("Cassandra");
        assertTrue(MusicUtil.getCassPwd().equals("Cassandra"));
    }

    @Test
    public void testGetAafEndpointUrl() {
        MusicUtil.setAafEndpointUrl("url");
        assertEquals(MusicUtil.getAafEndpointUrl(),"url");
    }

    @Test
    public void testGetPropkeys() {
        assertEquals(MusicUtil.getPropkeys()[2],"debug");
    }

    @Test
    public void testGetMusicPropertiesFilePath() {
        MusicUtil.setMusicPropertiesFilePath("filepath");
        assertEquals(MusicUtil.getMusicPropertiesFilePath(),"filepath");
    }

    @Test
    public void testGetDefaultLockLeasePeriod() {
        MusicUtil.setDefaultLockLeasePeriod(5000);
        assertEquals(MusicUtil.getDefaultLockLeasePeriod(),5000);
    }

    @Test
    public void testIsDebug() {
        MusicUtil.setDebug(true);
        assertTrue(MusicUtil.isDebug());
    }

    @Test
    public void testGetVersion() {
        MusicUtil.setVersion("1.0.0");
        assertEquals(MusicUtil.getVersion(),"1.0.0");
    }

    /*@Test
    public void testGetMyZkHost() {
        MusicUtil.setMyZkHost("10.0.0.2");
        assertEquals(MusicUtil.getMyZkHost(),"10.0.0.2");
    }*/

    @Test
    public void testGetMyCassaHost() {
        MusicUtil.setMyCassaHost("10.0.0.2");
        assertEquals(MusicUtil.getMyCassaHost(),"10.0.0.2");
    }

    @Test
    public void testGetDefaultMusicIp() {
        MusicUtil.setDefaultMusicIp("10.0.0.2");
        assertEquals(MusicUtil.getDefaultMusicIp(),"10.0.0.2");
    }

//    @Test
//    public void testGetTestType() {
//      fail("Not yet implemented"); // TODO
//    }

    @Test
    public void testIsValidQueryObject() {
        PreparedQueryObject myQueryObject = new PreparedQueryObject();
        myQueryObject.appendQueryString("select * from apple where type = ?");
        myQueryObject.addValue("macintosh");
        assertTrue(MusicUtil.isValidQueryObject(true,myQueryObject));

        myQueryObject.appendQueryString("select * from apple");
        assertTrue(MusicUtil.isValidQueryObject(false,myQueryObject));

        myQueryObject.appendQueryString("select * from apple where type = ?");
        assertFalse(MusicUtil.isValidQueryObject(true,myQueryObject));

        myQueryObject = new PreparedQueryObject();
        myQueryObject.appendQueryString("");
        System.out.println("#######" + myQueryObject.getQuery().isEmpty());
        assertFalse(MusicUtil.isValidQueryObject(false,myQueryObject));

    
    }

    @Test
    public void testConvertToCQLDataType() throws Exception {
        Map<String,Object> myMap = new HashMap<String,Object>();
        myMap.put("name","tom");
        assertEquals(MusicUtil.convertToCQLDataType(DataType.varchar(),"Happy People"),"'Happy People'");
        assertEquals(MusicUtil.convertToCQLDataType(DataType.uuid(),UUID.fromString("29dc2afa-c2c0-47ae-afae-e72a645308ab")),"29dc2afa-c2c0-47ae-afae-e72a645308ab");
        assertEquals(MusicUtil.convertToCQLDataType(DataType.blob(),"Hi"),"Hi");
        assertEquals(MusicUtil.convertToCQLDataType(DataType.map(DataType.varchar(),DataType.varchar()),myMap),"{'name':'tom'}");
    }

    @Test
    public void testConvertToActualDataType() throws Exception {
        assertEquals(MusicUtil.convertToActualDataType(DataType.varchar(),"Happy People"),"Happy People");
        assertEquals(MusicUtil.convertToActualDataType(DataType.uuid(),"29dc2afa-c2c0-47ae-afae-e72a645308ab"),UUID.fromString("29dc2afa-c2c0-47ae-afae-e72a645308ab"));
        assertEquals(MusicUtil.convertToActualDataType(DataType.varint(),"1234"),BigInteger.valueOf(Long.parseLong("1234")));
        assertEquals(MusicUtil.convertToActualDataType(DataType.bigint(),"123"),Long.parseLong("123"));
        assertEquals(MusicUtil.convertToActualDataType(DataType.cint(),"123"),Integer.parseInt("123"));
        assertEquals(MusicUtil.convertToActualDataType(DataType.cfloat(),"123.01"),Float.parseFloat("123.01"));
        assertEquals(MusicUtil.convertToActualDataType(DataType.cdouble(),"123.02"),Double.parseDouble("123.02"));
        assertEquals(MusicUtil.convertToActualDataType(DataType.cboolean(),"true"),Boolean.parseBoolean("true"));
        Map<String,Object> myMap = new HashMap<String,Object>();
        myMap.put("name","tom");
        assertEquals(MusicUtil.convertToActualDataType(DataType.map(DataType.varchar(),DataType.varchar()),myMap),myMap);

    }

    @Test
    public void testJsonMaptoSqlString() throws Exception {
        Map<String,Object> myMap = new HashMap<>();
        myMap.put("name","tom");
        myMap.put("value",5);
        String result = MusicUtil.jsonMaptoSqlString(myMap,",");
        assertTrue(result.contains("name"));
        assertTrue(result.contains("value"));
    }
    
    @Test
    public void test_generateUUID() {
        //this function shouldn't be in cachingUtil
        System.out.println("Testing getUUID");
        String uuid1 = MusicUtil.generateUUID();
        String uuid2 = MusicUtil.generateUUID();
        assertFalse(uuid1==uuid2);
    }


    @Test
    public void testIsValidConsistency(){
        assertTrue(MusicUtil.isValidConsistency("ALL"));
        assertFalse(MusicUtil.isValidConsistency("TEST"));
    }

    @Test
    public void testLockUsing() {
        MusicUtil.setLockUsing("testlock");
        assertEquals("testlock", MusicUtil.getLockUsing());
    }
    
    @Test
    public void testAAFAdminUrl() {
        MusicUtil.setAafAdminUrl("aafAdminURL.com");
        assertEquals("aafAdminURL.com", MusicUtil.getAafAdminUrl());
    }
    
    @Test
    public void testAAFEndpointUrl() {
        MusicUtil.setAafEndpointUrl("aafEndpointURL.com");
        assertEquals("aafEndpointURL.com", MusicUtil.getAafEndpointUrl());
    }
    
    @Test
    public void testNamespace() {
        MusicUtil.setMusicNamespace("musicNamespace");
        assertEquals("musicNamespace", MusicUtil.getMusicNamespace());
    }
    
    @Test
    public void testAAFRole() {
        MusicUtil.setAdminAafRole("aafRole");
        assertEquals("aafRole", MusicUtil.getAdminAafRole());
    }
    
    @Test
    public void testAdminId() {
        MusicUtil.setAdminId("adminId");
        assertEquals("adminId", MusicUtil.getAdminId());
    }
    
    @Test
    public void testAdminPass() {
        MusicUtil.setAdminPass("pass");
        assertEquals("pass", MusicUtil.getAdminPass());
    }
    
    @Test
    public void testCassaPort() {
        MusicUtil.setCassandraPort(1234);
        assertEquals(1234, MusicUtil.getCassandraPort());
    }
    
    @Test
    public void testBuild() {
        MusicUtil.setBuild("testbuild");
        assertEquals("testbuild", MusicUtil.getBuild());
    }
    
    @Test
    public void testNotifyInterval() {
        MusicUtil.setNotifyInterval(123);
        assertEquals(123, MusicUtil.getNotifyInterval());
    }
    
    @Test
    public void testNotifyTimeout() {
        MusicUtil.setNotifyTimeOut(789);
        assertEquals(789, MusicUtil.getNotifyTimeout());
    }
    
    @Test
    public void testTransId() {
        MusicUtil.setTransIdPrefix("prefix");
        assertEquals("prefix-", MusicUtil.getTransIdPrefix());
    }
    
    
    @Test
    public void testConversationIdPrefix() {
        MusicUtil.setConversationIdPrefix("prefix-");
        assertEquals("prefix-", MusicUtil.getConversationIdPrefix());
    }
    
    @Test
    public void testClientIdPrefix() {
        MusicUtil.setClientIdPrefix("clientIdPrefix");
        assertEquals("clientIdPrefix-", MusicUtil.getClientIdPrefix());
    }
    
    @Test
    public void testMessageIdPrefix() {
        MusicUtil.setMessageIdPrefix("clientIdPrefix");
        assertEquals("clientIdPrefix-", MusicUtil.getMessageIdPrefix());
    }
    
    @Test
    public void testTransIdPrefix() {
        MusicUtil.setTransIdPrefix("transIdPrefix");
        assertEquals("transIdPrefix-", MusicUtil.getTransIdPrefix());
    }
    
    @Test
    public void testconvIdReq() {
        MusicUtil.setConversationIdRequired("conversationIdRequired");
        assertEquals("conversationIdRequired", MusicUtil.getConversationIdRequired());
    }
    
    @Test
    public void testClientIdRequired() {
        MusicUtil.setClientIdRequired("conversationIdRequired");
        assertEquals("conversationIdRequired", MusicUtil.getClientIdRequired());
    }
    
    @Test
    public void testMessageIdRequired() {
        MusicUtil.setMessageIdRequired("msgIdRequired");
        assertEquals("msgIdRequired", MusicUtil.getMessageIdRequired());
    }
    
    @Test
    public void testLoadProperties() {
        PropertiesLoader pl = new PropertiesLoader();
        pl.loadProperties();
    }
    
}
