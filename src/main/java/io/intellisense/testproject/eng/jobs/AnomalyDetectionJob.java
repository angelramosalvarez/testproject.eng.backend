package io.intellisense.testproject.eng.jobs;


import io.intellisense.testproject.eng.function.AnomalyDetectionFunction;
import io.intellisense.testproject.eng.function.FilterIncompleteRowsFunction;
import io.intellisense.testproject.eng.function.TransformDataTypesFunction;
import io.intellisense.testproject.eng.model.DataPoint;
import lombok.extern.slf4j.Slf4j;
import io.intellisense.testproject.eng.datasource.CsvDatasource;
import io.intellisense.testproject.eng.sink.influxdb.InfluxDBSink;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;
import java.io.InputStream;
import java.time.Instant;

@Slf4j
public class AnomalyDetectionJob {

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
        env.setParallelism(configProperties.getInt("flink.parallelism", 1));
        env.getConfig().setGlobalJobParameters(configProperties);
        env.setBufferTimeout(100L); // configure buffer timeout



        //Workaround to solve deploy Flink jar in the cluster:
        //  If extarnal csv is provided as argument then get it
        String csvPath = null;
        if (programArgs.get("csvPath")!=null){
            csvPath =  programArgs.get("csvPath");
        }


        // Simple CSV-table datasource
        final String dataset = programArgs.get("sensorData", "sensor-data.csv");
        final CsvTableSource csvDataSource = CsvDatasource.of(dataset).getCsvSource(csvPath);
        final WatermarkStrategy<Row> watermarkStrategy = WatermarkStrategy.<Row>forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> timestampExtract(event));
        final DataStream<Row> sourceStream = csvDataSource.getDataStream(env)
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .name("datasource-operator");
        sourceStream.print();

         //datastream ETL
         final DataStream<Row> filteredStream = sourceStream.filter(new FilterIncompleteRowsFunction()).name("data-wrangling-operator");
         final DataStream<Tuple10<Double, Double, Double, Double, Double, Double, Double, Double, Double, Double>> mappedStream = filteredStream.map(new TransformDataTypesFunction()).name("mapping-operator");
         final DataStream<DataPoint> processedStream = mappedStream.countWindowAll(100, 100).process(new AnomalyDetectionFunction()).name("processing-operator");

        // Sink
        final SinkFunction<DataPoint> influxDBSink = new InfluxDBSink<>(configProperties);
        processedStream.addSink(influxDBSink).name("sink-operator");

        //processedStream.print();

        final JobExecutionResult jobResult = env.execute("Anomaly Detection Job");
        log.info(jobResult.toString());
    }

    private static long timestampExtract(Row event) {
        final String timestampField = (String) event.getField(0);
        return Instant.parse(timestampField).toEpochMilli();
    }
}
