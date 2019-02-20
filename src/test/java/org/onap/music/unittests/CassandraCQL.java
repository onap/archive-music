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

/**
 * @author srupane
 * 
 */

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

//import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.onap.music.datastore.MusicDataStore;
import org.onap.music.datastore.PreparedQueryObject;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class CassandraCQL {
	public static final String createAdminKeyspace = "CREATE KEYSPACE admin WITH REPLICATION = "
			+ "{'class' : 'SimpleStrategy' , 'replication_factor': 1} AND DURABLE_WRITES = true";
	
	public static final String createAdminTable = "CREATE TABLE admin.keyspace_master (" + "  uuid uuid, keyspace_name text,"
			+ "  application_name text, is_api boolean,"
			+ "  password text, username text,"
			+ "  is_aaf boolean, PRIMARY KEY (uuid)\n" + ");";
	
    public static final String createKeySpace =
                    "CREATE KEYSPACE IF NOT EXISTS testcassa WITH replication = "
                    +"{'class':'SimpleStrategy','replication_factor':1} AND durable_writes = true;";

    public static final String dropKeyspace = "DROP KEYSPACE IF EXISTS testcassa";

    public static final String createTableEmployees =
                    "CREATE TABLE IF NOT EXISTS testcassa.employees "
                    + "(vector_ts text,empid uuid,empname text,empsalary varint,address Map<text,text>,PRIMARY KEY (empname)) "
                    + "WITH comment='Financial Info of employees' "
                    + "AND compression={'sstable_compression':'DeflateCompressor','chunk_length_kb':64} "
                    + "AND compaction={'class':'SizeTieredCompactionStrategy','min_threshold':6};";

    public static final String insertIntoTablePrepared1 =
                    "INSERT INTO testcassa.employees (vector_ts,empid,empname,empsalary) VALUES (?,?,?,?); ";

    public static final String insertIntoTablePrepared2 =
                    "INSERT INTO testcassa.employees (vector_ts,empid,empname,empsalary,address) VALUES (?,?,?,?,?);";

    public static final String selectALL = "SELECT *  FROM testcassa.employees;";

    public static final String selectSpecific =
                    "SELECT *  FROM testcassa.employees WHERE empname= ?;";

    public static final String updatePreparedQuery =
                    "UPDATE testcassa.employees  SET vector_ts=?,address= ? WHERE empname= ?;";

    public static final String deleteFromTable = " ";

    public static final String deleteFromTablePrepared = " ";

    // Set Values for Prepared Query

    public static List<Object> setPreparedInsertValues1() {

        List<Object> preppreparedInsertValues1 = new ArrayList<>();
        String vectorTs =
                        String.valueOf(Thread.currentThread().getId() + System.currentTimeMillis());
        UUID empId = UUID.fromString("abc66ccc-d857-4e90-b1e5-df98a3d40cd6");
        BigInteger empSalary = BigInteger.valueOf(23443);
        String empName = "Mr Test one";
        preppreparedInsertValues1.add(vectorTs);
        preppreparedInsertValues1.add(empId);
        preppreparedInsertValues1.add(empName);
        preppreparedInsertValues1.add(empSalary);
        return preppreparedInsertValues1;
    }

    public static List<Object> setPreparedInsertValues2() {

        List<Object> preparedInsertValues2 = new ArrayList<>();
        String vectorTs =
                        String.valueOf(Thread.currentThread().getId() + System.currentTimeMillis());
        UUID empId = UUID.fromString("abc434cc-d657-4e90-b4e5-df4223d40cd6");
        BigInteger empSalary = BigInteger.valueOf(45655);
        String empName = "Mr Test two";
        Map<String, String> address = new HashMap<>();
        preparedInsertValues2.add(vectorTs);
        preparedInsertValues2.add(empId);
        preparedInsertValues2.add(empName);
        preparedInsertValues2.add(empSalary);
        address.put("Street", "1 some way");
        address.put("City", "Some town");
        preparedInsertValues2.add(address);
        return preparedInsertValues2;
    }

    public static List<Object> setPreparedUpdateValues() {

        List<Object> preparedUpdateValues = new ArrayList<>();
        String vectorTs =
                        String.valueOf(Thread.currentThread().getId() + System.currentTimeMillis());
        Map<String, String> address = new HashMap<>();
        preparedUpdateValues.add(vectorTs);
        String empName = "Mr Test one";
        address.put("Street", "101 Some Way");
        address.put("City", "New York");
        preparedUpdateValues.add(address);
        preparedUpdateValues.add(empName);
        return preparedUpdateValues;
    }

    // Generate Different Prepared Query Objects
    /**
     * Query Object for Get.
     * 
     * @return
     */
    public static PreparedQueryObject setPreparedGetQuery() {

        PreparedQueryObject queryObject = new PreparedQueryObject();
        String empName1 = "Mr Test one";
        queryObject.appendQueryString(selectSpecific);
        queryObject.addValue(empName1);
        return queryObject;
    }

    /**
     * Query Object 1 for Insert.
     * 
     * @return {@link PreparedQueryObject}
     */
    public static PreparedQueryObject setPreparedInsertQueryObject1() {

        PreparedQueryObject queryobject = new PreparedQueryObject();
        queryobject.appendQueryString(insertIntoTablePrepared1);
        List<Object> values = setPreparedInsertValues1();
        if (!values.isEmpty() || values != null) {
            for (Object o : values) {
                queryobject.addValue(o);
            }
        }
        return queryobject;

    }

    /**
     * Query Object 2 for Insert.
     * 
     * @return {@link PreparedQueryObject}
     */
    public static PreparedQueryObject setPreparedInsertQueryObject2() {

        PreparedQueryObject queryobject = new PreparedQueryObject();
        queryobject.appendQueryString(insertIntoTablePrepared2);
        List<Object> values = setPreparedInsertValues2();
        if (!values.isEmpty() || values != null) {
            for (Object o : values) {
                queryobject.addValue(o);
            }
        }
        return queryobject;

    }

    /**
     * Query Object for Update.
     * 
     * @return {@link PreparedQueryObject}
     */
    public static PreparedQueryObject setPreparedUpdateQueryObject() {

        PreparedQueryObject queryobject = new PreparedQueryObject();
        queryobject.appendQueryString(updatePreparedQuery);
        List<Object> values = setPreparedUpdateValues();
        if (!values.isEmpty() || values != null) {
            for (Object o : values) {
                queryobject.addValue(o);
            }
        }
        return queryobject;

    }

    private static ArrayList<String> getAllPossibleLocalIps() {
        ArrayList<String> allPossibleIps = new ArrayList<String>();
        try {
            Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces();
            while (en.hasMoreElements()) {
                NetworkInterface ni = (NetworkInterface) en.nextElement();
                Enumeration<InetAddress> ee = ni.getInetAddresses();
                while (ee.hasMoreElements()) {
                    InetAddress ia = (InetAddress) ee.nextElement();
                    allPossibleIps.add(ia.getHostAddress());
                }
            }
        } catch (SocketException e) {
            System.out.println(e.getMessage());
        }
        return allPossibleIps;
    }

    public static MusicDataStore connectToEmbeddedCassandra() throws Exception {
    	System.setProperty("log4j.configuration", "log4j.properties");
    	
        String address = "localhost";

        EmbeddedCassandraServerHelper.startEmbeddedCassandra();
        Cluster cluster = new Cluster.Builder().withoutJMXReporting().withoutMetrics().addContactPoint(address).withPort(9142).build();
        cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(5000);
        Session session = cluster.connect();
        
        return new MusicDataStore(cluster, session);
    }

}
