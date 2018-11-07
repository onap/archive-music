package org.onap.music.unittests;

import com.datastax.driver.core.ResultSet;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.exceptions.MusicLockingException;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.main.MusicCore;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class ComparisonPoints1
{
    String keyspaceName;
    int BENCH_TIMES = 100;

    public ComparisonPoints1() {
        keyspaceName = "ComparisonPoints1_"+System.currentTimeMillis();
    }

    private void initialize() throws MusicServiceException {
        createKeyspace();
        System.out.println("Created keyspaces");
    }

    private int readAge(String tableName, String name) throws MusicServiceException {
        PreparedQueryObject query = new PreparedQueryObject();
        query.appendQueryString("SELECT age FROM " + keyspaceName + "." + tableName + " WHERE name = ?;");
        query.addValue(name);
        ResultSet rs = MusicCore.get(query);
        return rs.one().getInt("age");
    }

    private void check(String tableName, String name1, int age) throws MusicServiceException {
        int readage = readAge(tableName, name1);
        if (age != readage)
            System.out.println("Inconsistency: age = " + readage + " != " + age);
    }

    private void performMusicEntryConsistentPut(String tableName, String name, int age) throws MusicServiceException, MusicLockingException, MusicQueryException {

        PreparedQueryObject query = new PreparedQueryObject();
        query.appendQueryString(
                "INSERT INTO " + keyspaceName + "." + tableName + " (name, age) "
                        + "VALUES (?, ?);");
        query.addValue(name);
        query.addValue(age);

        MusicCore.atomicPut(keyspaceName, tableName, name, query, null);
    }

    private void warmup() throws Exception {
        String tableName = "warmup";
        System.out.println("Warming Up");
        createTable(tableName);
        System.out.println("Created tables");
        Thread.sleep(1000);

        long btime = System.currentTimeMillis();
        for (int i = 0; i < BENCH_TIMES; i++) {
            String name = "Joe" + i;
            int age = i + 10;
            performMusicEntryConsistentPut(tableName, name, age);
//            check(tableName, name, age);
        }
        long bdur = System.currentTimeMillis() - btime;

        System.out.println("done");
    }

    private void testMusicEntryConsistentPut() throws Exception {
        String tableName = "mentry2";
        System.out.println("Test music entry consistent put 2");
        createTable(tableName);
        System.out.println("Created tables");
        Thread.sleep(1000);

        long btime = System.currentTimeMillis();
        for (int i = 0; i < BENCH_TIMES; i++) {
            String name = "Joe" + i;
            int age = i + 10;
            performMusicEntryConsistentPut(tableName, name, age);
//            check(tableName, name, age);
        }
        long bdur = System.currentTimeMillis() - btime;

        System.out.println("done " + ((float)bdur / BENCH_TIMES));
    }

    private void performMusicSequentialConsistentPut(String tableName, String name, int age) throws MusicServiceException, MusicLockingException, MusicQueryException {
        PreparedQueryObject query = new PreparedQueryObject();
        query.appendQueryString(
                "INSERT INTO " + keyspaceName + "." + tableName + " (name, age) "
                        + "VALUES (?, ?) IF NOT EXISTS;");
        query.addValue(name);
        query.addValue(age);

        MusicCore.atomicPut(keyspaceName, tableName, name, query, null);
    }

    private void testMusicSequentialConsistentPut() throws Exception {
        String tableName = "mseq";
        System.out.println("Test music sequential consistent put");
        createTable(tableName);
        System.out.println("Created tables");
        Thread.sleep(1000);

        long btime = System.currentTimeMillis();
        for (int i = 0; i < BENCH_TIMES; i++) {
            String name = "Joe" + i;
            int age = i + 10;
            performMusicSequentialConsistentPut(tableName, name, age);
//            check(tableName, name, age);
        }
        long bdur = System.currentTimeMillis() - btime;

        System.out.println("done " + ((float)bdur / BENCH_TIMES));
    }

    private void performEventualPut(String tableName, String name, int age) throws MusicServiceException, MusicLockingException, MusicQueryException {
        PreparedQueryObject query = new PreparedQueryObject();
        query.appendQueryString(
                "INSERT INTO " + keyspaceName + "." + tableName + " (name, age) "
                        + "VALUES (?, ?);");
        query.addValue(name);
        query.addValue(age);

        MusicCore.eventualPut(query);
    }

    private void testEventualPut() throws Exception {
        String tableName = "eventual";
        System.out.println("Test eventual put");
        createTable(tableName);
        System.out.println("Created tables");
        Thread.sleep(1000);

        long btime = System.currentTimeMillis();
        for (int i = 0; i < BENCH_TIMES; i++) {
            String name = "Joe" + i;
            int age = i + 10;
            performEventualPut(tableName, name, age);
//            check(tableName, name, age);
        }
        long bdur = System.currentTimeMillis() - btime;

        System.out.println("done " + ((float)bdur / BENCH_TIMES));
    }

    private void performPureConsistentPut(String tableName, String name, int age) throws MusicServiceException, MusicLockingException, MusicQueryException {
        PreparedQueryObject query = new PreparedQueryObject();
        query.appendQueryString(
                "INSERT INTO " + keyspaceName + "." + tableName + " (name, age) "
                        + "VALUES (?, ?) IF NOT EXISTS;");
        query.addValue(name);
        query.addValue(age);

        MusicCore.eventualPut(query);
    }

    private void testPureConsistentPut() throws Exception {
        String tableName = "pure";
        System.out.println("Performing pure consistent put");
        createTable(tableName);
        System.out.println("Created tables");
        Thread.sleep(1000);

        long btime = System.currentTimeMillis();
        for (int i = 0; i < BENCH_TIMES; i++) {
            String name = "Joe" + i;
            int age = i + 10;
            performPureConsistentPut(tableName, name, age);
//            check(tableName, name, age);
        }
        long bdur = System.currentTimeMillis() - btime;

        System.out.println("done " + ((float)bdur / BENCH_TIMES));
    }

    private void createKeyspace() throws MusicServiceException {
              Map<String,Object> replicationInfo = new HashMap<String, Object>();
              replicationInfo.put("'class'", "'SimpleStrategy'");
              replicationInfo.put("'replication_factor'", 3);
              
              PreparedQueryObject queryObject = new PreparedQueryObject();
              queryObject.appendQueryString(
                "CREATE KEYSPACE " + keyspaceName + " WITH REPLICATION = " + replicationInfo.toString().replaceAll("=", ":"));
              
              try {
                     MusicCore.nonKeyRelatedPut(queryObject, "eventual");
              } catch (MusicServiceException e) {
                   throw(e);
              }
       }
       
    private void createTable(String tableName) throws MusicServiceException {
        PreparedQueryObject queryObject = new PreparedQueryObject();
        queryObject.appendQueryString(
                "CREATE TABLE " + keyspaceName + "." + tableName + " (name text PRIMARY KEY, age int);");

        try {
            MusicCore.createTable(keyspaceName, tableName, queryObject, "eventual");
        } catch (MusicServiceException e) {
            throw (e);
        }
    }

       public static void main( String[] args ) throws Exception {
           ComparisonPoints1 cp1 = new ComparisonPoints1();
           cp1.initialize();
           Thread.sleep(2000);

           cp1.warmup();
           System.out.println("-----\n\n");
           Thread.sleep(2000);

           cp1.testEventualPut();
           System.out.println("-----\n\n");
           Thread.sleep(2000);

           cp1.testMusicSequentialConsistentPut();
           System.out.println("-----\n\n");
           Thread.sleep(2000);

           cp1.testPureConsistentPut();
           System.out.println("-----\n\n");
           Thread.sleep(2000);

           cp1.testMusicEntryConsistentPut();
           System.out.println("-----\n\n");

           System.exit(0);
    }
 
}