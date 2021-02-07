package io.intellisense.testproject.eng.jobs;

import io.intellisense.testproject.eng.datasource.CsvDatasource;
import io.intellisense.testproject.eng.function.InterquartileComputer;
import io.intellisense.testproject.eng.function.SensorRead;
import io.intellisense.testproject.eng.function.Splitter;
import io.intellisense.testproject.eng.model.DataPoint;
import io.intellisense.testproject.eng.sink.influxdb.InfluxDBSink;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;

import java.io.InputStream;
import java.time.Instant;

@Slf4j
public class AnomalyDetectionJob {

    public static final Integer NUM_SENSORS = 10;

    public static void main(String[] args) throws Exception {

        // Pass configFile location as a program argument:
        // --configFile config.local.yaml
        final ParameterTool programArgs = ParameterTool.fromArgs(args);
        final String configFile = programArgs.getRequired("configFile");
        final InputStream resourceStream = AnomalyDetectionJob.class.getClassLoader().getResourceAsStream(configFile);
        final ParameterTool configProperties = ParameterTool.fromPropertiesFile(resourceStream);

        // Stream execution environment
        // ...you can add here whatever you consider necessary for robustness
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // We could increase this value to use more than one worker.
        env.setParallelism(configProperties.getInt("flink.parallelism", 1));
        env.getConfig().setGlobalJobParameters(configProperties);

        // Simple CSV-table datasource
        final String dataset = programArgs.get("sensorData", "sensor-data.csv");
        final CsvTableSource csvDataSource = CsvDatasource.of(dataset).getCsvSource();
        final WatermarkStrategy<Row> watermarkStrategy = WatermarkStrategy.<Row>forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> timestampExtract(event));
        final DataStream<Row> sourceStream = csvDataSource.getDataStream(env)
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .name("datasource-operator");
        // Split the csv line with 10 sensors to individual sensor readings
        final DataStream<SensorRead> splitSensors = Splitter.getSplitSensors(sourceStream);

        // Key - Value the sensor readings, so each reading ends up in one partition.
        final KeyedStream<SensorRead, String> partitioned = splitSensors.keyBy(value -> value.getSensor());

        // Compute DataPoints for each reading, with the score based on the iqr for las 100 readings
        SingleOutputStreamOperator<DataPoint> scoredSensorReading = partitioned.map(new InterquartileComputer());

        // Sink
        final SinkFunction<DataPoint> influxDBSink = new InfluxDBSink<>(configProperties);
        scoredSensorReading.addSink(influxDBSink).name("sink-operator");

        final JobExecutionResult jobResult = env.execute("Anomaly Detection Job");
        log.info(jobResult.toString());
    }

    private static long timestampExtract(Row event) {
        final String timestampField = (String) event.getField(0);
        return Instant.parse(timestampField).toEpochMilli();
    }
}
