package org.onap.music.util;

import com.google.common.math.Quantiles;
import com.google.common.math.Stats;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class ReserviorSampler {
    private double[] samples;
    private long length;
    private int size;

    public ReserviorSampler(int size) {
        this.samples = new double[size];
        this.size = size;
        this.length = 0;
    }

    public void insert(double x) {
        if (length < size) {
            samples[(int)length] = x;
            length++;
        }
        else {
            long r = ThreadLocalRandom.current().nextLong(length);
            if (r < size) {
                samples[(int)r] = x;
            }
        }
    }

    public double[] getSamples() {
        if (length < size)
            return Arrays.copyOfRange(samples, 0, (int)length);
        else
            return samples;
    }
}

public class SamplerHistogramTimeMeasure implements TimeMeasure
{
    public static final int SAMPLER_SIZE = 1000;
    private Map<String, ReserviorSampler> histograms;
    private ThreadLocal<LinkedList<Pair<String, Long>>> tlContexts;
    int[] p100;

    public SamplerHistogramTimeMeasure() {
        histograms = new HashMap<>();
        tlContexts = ThreadLocal.withInitial(() -> new LinkedList<>());
        p100 = IntStream.rangeClosed(0, 100)
                .toArray();
    }

    public void init() {
        tlContexts.get();
    }

    @Override
    public void enter(String context) {
        LinkedList<Pair<String, Long>> contexts = tlContexts.get();
        String concatContext = (contexts.size() > 0 ? contexts.getLast().getLeft() + "." : "") + context;
        contexts.add(new ImmutablePair<>(concatContext, System.nanoTime()));
    }

    @Override
    public void exit() {
        long nanoTime = System.nanoTime();
        LinkedList<Pair<String, Long>> contexts = tlContexts.get();
        Pair<String, Long> e = contexts.removeLast();
        double t = (nanoTime - e.getRight()) * 1e-6;
        ReserviorSampler h = histograms.computeIfAbsent(e.getLeft(), k -> new ReserviorSampler(SAMPLER_SIZE));
        h.insert(t);
    }

    @Override
    public Map<String, ArrayList<Double>> percentiles() {
        Map<String, ArrayList<Double>> mapped = histograms.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                    entry -> new ArrayList<>(new TreeMap<>(
                        Quantiles.percentiles().indexes(p100).compute(entry.getValue().getSamples())
                    ).values())
                ));
        return mapped;
    }

    @Override
    public Map<String, Pair<Double, Double>> stats(){
        Map<String, Pair<Double, Double>> mapped = histograms.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                        entry -> {
                            Stats s = Stats.of(entry.getValue().getSamples());
                            if (s.count() <= SAMPLER_SIZE)
                                return new ImmutablePair<>(s.mean(), s.populationStandardDeviation() / s.count());
                            else
                                return new ImmutablePair<>(s.mean(), s.sampleStandardDeviation() / s.count());

                        }
                ));
        return mapped;
    }
}
