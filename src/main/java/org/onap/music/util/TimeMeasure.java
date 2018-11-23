package org.onap.music.util;

import org.apache.commons.lang3.tuple.Pair;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public interface TimeMeasure {
    void enter(String context);

    void exit();

    /*
        Return a map of measure contexts to a list of 101 percentiles [0,100]
     */
    Map<String, ArrayList<Double>> percentiles();

    /*
        Returns a map of measure contexts to <mean, sme>
     */
    Map<String, Pair<Double, Double>> stats();
}

class TimeMeasureExample
{
    public static void main(String[] args) {
        TimeMeasure tm = new SamplerHistogramTimeMeasure();
        double x = 0;

        tm.enter("A");
        for (int i = 0; i < 100000; i++) {
            tm.enter("B");
            tm.enter("C");
            x += ThreadLocalRandom.current().nextDouble(100);
            tm.exit();
            tm.exit();
        }
        tm.enter("C");
        tm.exit();
        tm.exit();

        System.out.println(x);
        Map<String, ArrayList<Double>> e = tm.percentiles();
        Map<String, Pair<Double, Double>> m = tm.stats();
        DecimalFormat df = new DecimalFormat("000.000000");
        e.forEach((k,v) -> System.out.println("" + k + "\t\t: " + Arrays.toString(v.stream().map(w -> "" + df.format(w)).toArray())));
        m.forEach((k,v) -> System.out.println("" + k + "\t\t: " + df.format(v.getLeft()) + " (" + df.format(v.getRight()) + ")"));
    }
}
