package io.intellisense.testproject.eng.function;

import io.intellisense.testproject.eng.jobs.AnomalyDetectionJob;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Instant;

public class Splitter {

    /**
     * This method will get several sensor readings together, and split them into individual sensor readings.
     * @param sourceStream
     * @return
     */
    public static SingleOutputStreamOperator<SensorRead> getSplitSensors(DataStream<Row> sourceStream) {
        return sourceStream.flatMap(new FlatMapFunction<Row, SensorRead>() {
            @Override
            public void flatMap(Row value, Collector<SensorRead> out)
                    throws Exception {
                final String timestampField = (String) value.getField(0);
                long milli = Instant.parse(timestampField).toEpochMilli();
                for (int i = 1; i <= AnomalyDetectionJob.NUM_SENSORS; i++) {
                    final String mValue = String.valueOf(value.getField(i));
                    double read =0;
                    //Catching wrong values. A side output would be good for follow up
                    try {
                        read = Double.parseDouble(mValue);
                    } catch (Exception e) {
                        read= 0;
                    }
                    out.collect(new SensorRead(milli, "Sensor-" + (i), read));
                }
            }
        });
    }
}
