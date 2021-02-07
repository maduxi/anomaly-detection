package io.intellisense.testproject.eng.function;

import io.intellisense.testproject.eng.model.DataPoint;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;


public class InterquartileComputer extends RichMapFunction<SensorRead, DataPoint> {

    final int MIN_DATAPOINTS = 100;
    private transient ValueState<ArrayList<Double>> lastItems;

    @Override
    public DataPoint map(SensorRead sensorRead) throws Exception {
        ArrayList<Double> value = lastItems.value();
        DataPoint dp;
        Instant time = new Timestamp(sensorRead.getTime()).toInstant();
        //We dont want to give false alerts based on few samples

        if (value.size() < MIN_DATAPOINTS) {
            value.add(0, sensorRead.getRead());
            lastItems.update(value);
            dp = new DataPoint(sensorRead.getSensor(), time, 0.5d);
        } else {
            double iqr = getIqr(value);
            value.remove(value.size() - 1);
            value.add(0, sensorRead.getRead());
            lastItems.update(value);
            dp = new DataPoint(sensorRead.getSensor(), time, getScore(iqr));
        }
        return dp;
    }

    private Double getScore(double iqr) {
        Double result;
        if (iqr < 1.5) {
            result = 0D;
        } else if (iqr >= 3) {
            result = 1D;
        } else {
            result = 0.5D;
        }
        return result;
    }

    private double getIqr(List<Double> items) {
        double[] data = items.stream().filter(i -> i != null).mapToDouble(i -> i).toArray();
        DescriptiveStatistics da = new DescriptiveStatistics(data);
        return da.getPercentile(75) - da.getPercentile(25);
    }

    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<ArrayList<Double>> descriptor =
                new ValueStateDescriptor<ArrayList<Double>>(
                        "lastItems", // the state name
                        TypeInformation.of(new TypeHint<ArrayList<Double>>() {
                        }), // type information
                        new ArrayList<Double>()); // default value of the state, if nothing was set
        lastItems = getRuntimeContext().getState(descriptor);
    }
}
