package org.onap.music.util;

public class TimeMeasureInstance {
    private static TimeMeasure instance = new NullTimeMeasure();

    public static TimeMeasure instance() {
        return TimeMeasureInstance.instance;
    }

    public static void setInstance(TimeMeasure instance) {
        TimeMeasureInstance.instance = instance;
    }
}
