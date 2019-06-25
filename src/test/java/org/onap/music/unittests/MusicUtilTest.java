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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Test;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.main.MusicUtil;
import org.onap.music.main.PropertiesLoader;
import org.onap.music.service.MusicCoreService;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;

public class MusicUtilTest {

    private static final String XLATESTVERSION = "X-latestVersion";
    private static final String XMINORVERSION = "X-minorVersion";
    private static final String XPATCHVERSION = "X-patchVersion";

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
    public void testMusicAafNs() {
        MusicUtil.setMusicAafNs("ns");
        assertTrue("ns".equals(MusicUtil.getMusicAafNs()));
    }

    @Test
    public void testMusicCoreService() {
        MusicUtil.setLockUsing(MusicUtil.CASSANDRA);
        MusicCoreService mc = null;
        mc = MusicUtil.getMusicCoreService();
        assertTrue(mc != null);        
        MusicUtil.setLockUsing("nothing");
        mc = null;
        mc = MusicUtil.getMusicCoreService();
        assertTrue(mc != null);        
        
    }

    @Test
    public void testCipherEncKey() {
        MusicUtil.setCipherEncKey("cipherEncKey");
        assertTrue("cipherEncKey".equals(MusicUtil.getCipherEncKey()));        
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

    @Test 
    public void testBuildVersionA() {
        assertEquals(MusicUtil.buildVersion("1","2","3"),"1.2.3");
    }

    @Test 
    public void testBuildVersionB() {
        assertEquals(MusicUtil.buildVersion("1",null,"3"),"1");
    }

    @Test 
    public void testBuildVersionC() {
        assertEquals(MusicUtil.buildVersion("1","2",null),"1.2");
    }


    @Test
    public void testBuileVersionResponse() {
        assertTrue(MusicUtil.buildVersionResponse("1","2","3").getClass().getSimpleName().equals("Builder"));
        assertTrue(MusicUtil.buildVersionResponse("1",null,"3").getClass().getSimpleName().equals("Builder"));
        assertTrue(MusicUtil.buildVersionResponse("1","2",null).getClass().getSimpleName().equals("Builder"));
        assertTrue(MusicUtil.buildVersionResponse(null,null,null).getClass().getSimpleName().equals("Builder"));
    }

    @Test
    public void testGetConsistency() {
        assertTrue(ConsistencyLevel.ONE.equals(MusicUtil.getConsistencyLevel("one")));
    }

    @Test
    public void testRetryCount() {
        MusicUtil.setRetryCount(1);
        assertEquals(MusicUtil.getRetryCount(),1);
    }

    @Test
    public void testIsCadi() {
        MusicUtil.setIsCadi(true);
        assertEquals(MusicUtil.getIsCadi(),true);
    }


    @Test
    public void testGetMyCassaHost() {
        MusicUtil.setMyCassaHost("10.0.0.2");
        assertEquals(MusicUtil.getMyCassaHost(),"10.0.0.2");
    }

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




    @Test(expected = IllegalStateException.class)
    public void testMusicUtil() {
        System.out.println("MusicUtil Constructor Test");
        MusicUtil mu = new MusicUtil();
        System.out.println(mu.toString());
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
        List<String> myList = new ArrayList<String>();
        List<String> newList = myList;
        myList.add("TOM");
        assertEquals(MusicUtil.convertToActualDataType(DataType.list(DataType.varchar()),myList),newList);
        Map<String,Object> myMap = new HashMap<String,Object>();
        myMap.put("name","tom");
        Map<String,Object> newMap = myMap;
        assertEquals(MusicUtil.convertToActualDataType(DataType.map(DataType.varchar(),DataType.varchar()),myMap),newMap);
    }

    @Test
    public void testConvertToActualDataTypeByte() throws Exception {
        byte[] testByte = "TOM".getBytes();
        assertEquals(MusicUtil.convertToActualDataType(DataType.blob(),testByte),ByteBuffer.wrap(testByte));

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
    public void testConvIdReq() {
        MusicUtil.setConversationIdRequired(true);
        assertEquals(true, MusicUtil.getConversationIdRequired());
    }
    
    @Test
    public void testClientIdRequired() {
        MusicUtil.setClientIdRequired(true);
        assertEquals(true, MusicUtil.getClientIdRequired());
    }
    
    @Test
    public void testMessageIdRequired() {
        MusicUtil.setMessageIdRequired(true);
        assertEquals(true, MusicUtil.getMessageIdRequired());
    }

    @Test
    public void testTransIdRequired() {
        MusicUtil.setTransIdRequired(true);
        assertEquals(true,MusicUtil.getTransIdRequired());
    }

    @Test
    public void testLoadProperties() {
        PropertiesLoader pl = new PropertiesLoader();
        pl.loadProperties();
    }
    
}
