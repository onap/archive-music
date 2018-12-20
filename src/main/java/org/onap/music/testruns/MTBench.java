package org.onap.music.testruns;

import com.datastax.driver.core.ResultSet;
import org.apache.commons.lang3.tuple.Pair;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.exceptions.MusicLockingException;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.main.MusicCore;
import org.onap.music.main.MusicUtil;
import org.onap.music.util.SamplerHistogramTimeMeasure;
import org.onap.music.util.TimeMeasure;
import org.onap.music.util.TimeMeasureInstance;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Function;


@FunctionalInterface
interface TriFunction<T,U,S> {
    void apply(T t, U u, S s);
}

public class MTBench
{
    public static final int NTHREAD = 1;
    public static final int BENCH_TIMES = 100;
    String keyspaceName;

    public MTBench() {
        keyspaceName = "MTBench_"+System.currentTimeMillis();
    }

    private void initialize() throws MusicServiceException {
        createKeyspace();
        System.out.println("Created keyspace: " + keyspaceName);
    }

    private void performWarmup(String tableName, String name, int age) {
        PreparedQueryObject query = new PreparedQueryObject();
        query.appendQueryString(
                "INSERT INTO " + keyspaceName + "." + tableName + " (name, age) "
                        + "VALUES (?, ?);");
        query.addValue(name);
        query.addValue(age);

        MusicCore.eventualPut(query);
    }

    private void warmup() throws Exception {
        TimeMeasureInstance.instance().enter("warmup");
        String tableName = "warmup";
        System.out.println("Warming Up");
        createTable(tableName);
        Thread.sleep(1000);

        for (int i = 0; i < 20; i++) {
            String name = "Joe" + i;
            int age = i + 12300;
            performWarmup(tableName, name, age);
//            check(tableName, name, age);
        }

        System.out.println("done");
        TimeMeasureInstance.instance().exit();
    }

    private void performMusicEntryConsistentPut(String tableName, String name, int age) {
        TimeMeasureInstance.instance().enter("performMusicEntryConsistentPut");
        PreparedQueryObject query = new PreparedQueryObject();
        query.appendQueryString(
                "INSERT INTO " + keyspaceName + "." + tableName + " (name, age) "
                        + "VALUES (?, ?);");
        query.addValue(name);
        query.addValue(age);

        try {
            MusicCore.atomicPut(keyspaceName, tableName, name, query, null);
        } catch (MusicLockingException e) {
            e.printStackTrace();
        } catch (MusicQueryException e) {
            e.printStackTrace();
        } catch (MusicServiceException e) {
            e.printStackTrace();
        }
        TimeMeasureInstance.instance().exit();
    }

    private void performMusicSequentialConsistentPut(String tableName, String name, int age) {
        TimeMeasureInstance.instance().enter("performMusicSequentialConsistentPut");
        PreparedQueryObject query = new PreparedQueryObject();
        query.appendQueryString(
                "INSERT INTO " + keyspaceName + "." + tableName + " (name, age) "
                        + "VALUES (?, ?) IF NOT EXISTS;");
        query.addValue(name);
        query.addValue(age);

        try {
            MusicCore.atomicPut(keyspaceName, tableName, name, query, null);
        } catch (MusicLockingException e) {
            e.printStackTrace();
        } catch (MusicQueryException e) {
            e.printStackTrace();
        } catch (MusicServiceException e) {
            e.printStackTrace();
        }
        TimeMeasureInstance.instance().exit();
    }

    private void performEventualPut(String tableName, String name, int age) {
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

    private void performPureConsistentPut(String tableName, String name, int age) {
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

    private void benchmark(String benchName, TriFunction<String, String, Integer> benchFunc) throws Exception {
        String tableName = "bm" + benchName;
        System.out.println("Benchmark " + benchName + " NTHREAD: " + NTHREAD + " BT: " + BENCH_TIMES);
        createTable(tableName);
        Thread.sleep(1000);
        System.out.println("begin");

        CountDownLatch cdl = new CountDownLatch(NTHREAD);
        ExecutorService es = Executors.newFixedThreadPool(NTHREAD);

        TimeMeasureInstance.setInstance(new SamplerHistogramTimeMeasure());
        TimeMeasure tm = TimeMeasureInstance.instance();

        tm.enter("benchmark" + benchName);
        for (int i = 0; i < NTHREAD; i++) {
            int finalI = i;
            Runnable task = () -> {
                try {
                    for (int j = 0; j < BENCH_TIMES; j++) {
                        int age = finalI * (BENCH_TIMES + 1000) + 10 + j;
                        String name = "Joe" + age;
                        benchFunc.apply(tableName, name, age);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    cdl.countDown();
                }
            };

            es.execute(task);
        }

        cdl.await();
        tm.exit();

        System.out.println("done");

        Map<String, ArrayList<Double>> e = tm.percentiles();
        Map<String, Pair<Double, Double>> m = tm.stats();
        DecimalFormat df = new DecimalFormat("000.000");
//           e.forEach((k,v) -> System.out.println("" + k + "\t\t: " + Arrays.toString(v.stream().map(w -> "" + df.format(w)).toArray())));
        m.forEach((k,v) -> System.out.println("" + k + "," + df.format(v.getLeft()) + "," + df.format(v.getRight()) + ""));
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

       public static void main(String[] args) throws Exception {
           if (args.length > 0) {
               MusicUtil.setMyCassaHost(args[0]);
           }
           MTBench cp1 = new MTBench();
           cp1.initialize();
           Thread.sleep(2000);

           cp1.warmup();
           System.out.println("-----\n\n");

           TriFunction<String, String, Integer> performEventualPut = cp1::performEventualPut;
           TriFunction<String, String, Integer> performPureConsistentPut = cp1::performPureConsistentPut;
           TriFunction<String, String, Integer> performMusicEntryConsistentPut = cp1::performMusicEntryConsistentPut;
           TriFunction<String, String, Integer> performMusicSequentialConsistentPut = cp1::performMusicSequentialConsistentPut;


           Thread.sleep(1000);
           cp1.benchmark("MusicEntryConsistentPut", performMusicEntryConsistentPut);
           System.out.println("-----\n\n");
           Thread.sleep(1000);
//
//           cp1.benchmarkMusicSequentialConsistentPut();
//           System.out.println("-----\n\n");
//           Thread.sleep(1000);
//
//           cp1.benchmarkPureConsistentPut();
//           System.out.println("-----\n\n");
//           Thread.sleep(1000);
//
//           cp1.benchmarkMusicEntryConsistentPut();
//           System.out.println("-----\n\n");


           System.exit(0);
    }
 
}