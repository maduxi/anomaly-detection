package io.intellisense.testproject.eng.function;

public class SensorRead {
    private long time;
    private String sensor;
    private Double read;

    public SensorRead(long time, String sensor, Double read) {
        this.time = time;
        this.sensor = sensor;
        this.read = read;
    }

    public Double getRead() {
        return read;
    }

    public String getSensor() {
        return sensor;
    }

    public long getTime() {
        return time;
    }
}
