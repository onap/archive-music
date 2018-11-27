package org.onap.music.testruns;

import com.datastax.driver.core.ResultSet;
import org.apache.commons.lang3.tuple.Pair;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.exceptions.MusicLockingException;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.main.MusicCore;
import org.onap.music.util.SamplerHistogramTimeMeasure;
import org.onap.music.util.TimeMeasure;
import org.onap.music.util.TimeMeasureInstance;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
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

    private void performWarmup(String tableName, String name, int age) throws MusicServiceException, MusicLockingException, MusicQueryException {
        PreparedQueryObject query = new PreparedQueryObject();
        query.appendQueryString(
                "INSERT INTO " + keyspaceName + "." + tableName + " (name, age) "
                        + "VALUES (?, ?);");
        query.addValue(name);
        query.addValue(age);

        MusicCore.atomicPut(keyspaceName, tableName, name, query, null);
    }



    private void warmup() throws Exception {
        TimeMeasureInstance.instance().enter("warmup");
        String tableName = "warmup";
        System.out.println("Warming Up");
        createTable(tableName);
        Thread.sleep(1000);

        for (int i = 0; i < BENCH_TIMES; i++) {
            String name = "Joe" + i;
            int age = i + 12300;
            performWarmup(tableName, name, age);
//            check(tableName, name, age);
        }

        System.out.println("done");
        TimeMeasureInstance.instance().exit();
    }

    private void performMusicEntryConsistentPut(String tableName, String name, int age) throws MusicServiceException, MusicLockingException, MusicQueryException {
        TimeMeasureInstance.instance().enter("performMusicEntryConsistentPut");
        PreparedQueryObject query = new PreparedQueryObject();
        query.appendQueryString(
                "INSERT INTO " + keyspaceName + "." + tableName + " (name, age) "
                        + "VALUES (?, ?);");
        query.addValue(name);
        query.addValue(age);

        MusicCore.atomicPut(keyspaceName, tableName, name, query, null);
        TimeMeasureInstance.instance().exit();
    }

    private void benchmarkMusicEntryConsistentPut() throws Exception {
        TimeMeasureInstance.instance().enter("benchmarkMusicEntryConsistentPut");
        String tableName = "mentry2";
        System.out.println("Benchmark music entry consistent put");
        createTable(tableName);
        Thread.sleep(1000);
        System.out.println("begin");

        for (int i = 0; i < BENCH_TIMES; i++) {
            String name = "Joe" + i;
            int age = i + 10;
            performMusicEntryConsistentPut(tableName, name, age);
//            check(tableName, name, age);
        }

        System.out.println("done");
        TimeMeasureInstance.instance().exit();
    }

    private void performMusicSequentialConsistentPut(String tableName, String name, int age) throws MusicServiceException, MusicLockingException, MusicQueryException {
        TimeMeasureInstance.instance().enter("performMusicSequentialConsistentPut");
        PreparedQueryObject query = new PreparedQueryObject();
        query.appendQueryString(
                "INSERT INTO " + keyspaceName + "." + tableName + " (name, age) "
                        + "VALUES (?, ?) IF NOT EXISTS;");
        query.addValue(name);
        query.addValue(age);

        MusicCore.atomicPut(keyspaceName, tableName, name, query, null);
        TimeMeasureInstance.instance().exit();
    }

    private void benchmarkMusicSequentialConsistentPut() throws Exception {
        TimeMeasureInstance.instance().enter("benchmarkMusicSequentialConsistentPut");
        String tableName = "mseq";
        System.out.println("Benchmark music sequential consistent put");
        createTable(tableName);
        Thread.sleep(1000);
        System.out.println("begin");

        for (int i = 0; i < BENCH_TIMES; i++) {
            String name = "Joe" + i;
            int age = i + 10;
            performMusicSequentialConsistentPut(tableName, name, age);
//            check(tableName, name, age);
        }

        System.out.println("done");
        TimeMeasureInstance.instance().exit();
    }

    private void performEventualPut(String tableName, String name, int age) throws MusicServiceException, MusicLockingException, MusicQueryException {
        TimeMeasureInstance.instance().enter("performEventualPut");
        PreparedQueryObject query = new PreparedQueryObject();
        query.appendQueryString(
                "INSERT INTO " + keyspaceName + "." + tableName + " (name, age) "
                        + "VALUES (?, ?);");
        query.addValue(name);
        query.addValue(age);

        MusicCore.eventualPut(query);
        TimeMeasureInstance.instance().exit();
    }

    private void benchmarkEventualPut() throws Exception {
        TimeMeasureInstance.instance().enter("benchmarkEventualPut");
        String tableName = "eventual";
        System.out.println("Benchmark eventual put");
        createTable(tableName);
        Thread.sleep(1000);
        System.out.println("begin");

        for (int i = 0; i < BENCH_TIMES; i++) {
            String name = "Joe" + i;
            int age = i + 10;
            performEventualPut(tableName, name, age);
//            check(tableName, name, age);
        }
        System.out.println("done");
        TimeMeasureInstance.instance().exit();
    }

    private void performPureConsistentPut(String tableName, String name, int age) throws MusicServiceException, MusicLockingException, MusicQueryException {
        TimeMeasureInstance.instance().enter("performPureConsistentPut");
        PreparedQueryObject query = new PreparedQueryObject();
        query.appendQueryString(
                "INSERT INTO " + keyspaceName + "." + tableName + " (name, age) "
                        + "VALUES (?, ?) IF NOT EXISTS;");
        query.addValue(name);
        query.addValue(age);

        MusicCore.eventualPut(query);
        TimeMeasureInstance.instance().exit();
    }

    private void benchmarkPureConsistentPut() throws Exception {
        TimeMeasureInstance.instance().enter("benchmarkPureConsistentPut");
        String tableName = "pure";
        System.out.println("Benchmark pure consistent put");
        createTable(tableName);
        Thread.sleep(1000);
        System.out.println("begin");

        for (int i = 0; i < BENCH_TIMES; i++) {
            String name = "Joe" + i;
            int age = i + 10;
            performPureConsistentPut(tableName, name, age);
//            check(tableName, name, age);
        }

        System.out.println("done");
        TimeMeasureInstance.instance().exit();
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
           TimeMeasureInstance.setInstance(new SamplerHistogramTimeMeasure());
           ComparisonPoints1 cp1 = new ComparisonPoints1();
           cp1.initialize();
           Thread.sleep(2000);

           cp1.warmup();
           System.out.println("-----\n\n");
           Thread.sleep(1000);

           cp1.benchmarkEventualPut();
           System.out.println("-----\n\n");
           Thread.sleep(1000);

           cp1.benchmarkMusicSequentialConsistentPut();
           System.out.println("-----\n\n");
           Thread.sleep(1000);

           cp1.benchmarkPureConsistentPut();
           System.out.println("-----\n\n");
           Thread.sleep(1000);

           cp1.benchmarkMusicEntryConsistentPut();
           System.out.println("-----\n\n");

           TimeMeasure tm = TimeMeasureInstance.instance();

           Map<String, ArrayList<Double>> e = tm.percentiles();
           Map<String, Pair<Double, Double>> m = tm.stats();
           DecimalFormat df = new DecimalFormat("000.000000");
           e.forEach((k,v) -> System.out.println("" + k + "\t\t: " + Arrays.toString(v.stream().map(w -> "" + df.format(w)).toArray())));
           m.forEach((k,v) -> System.out.println("" + k + "\t\t: " + df.format(v.getLeft()) + " (" + df.format(v.getRight()) + ")"));

           System.exit(0);
    }
 
}