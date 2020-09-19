package domain;

import lombok.Data;

@Data
public class SensorReading {
    public String id;
    public long ts;
    public double tp;

    public SensorReading() {}

    public SensorReading(String _id, long _ts, double _tp) {
        this.id = _id;
        this.ts = _ts;
        this.tp = _tp;
    }
}