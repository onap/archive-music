package org.onap.music.util;

import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Map;

public class NullTimeMeasure implements TimeMeasure
{
    public NullTimeMeasure() {
    }

    public void enter(String context) {
    }

    public void exit() {
    }

    @Override
    public Map<String, ArrayList<Double>> percentiles()
    {
        return null;
    }

    @Override
    public Map<String, Pair<Double, Double>> stats() {
        return null;
    }
}
