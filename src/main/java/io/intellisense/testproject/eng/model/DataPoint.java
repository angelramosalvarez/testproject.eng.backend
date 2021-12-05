package io.intellisense.testproject.eng.model;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.influxdb.annotation.Column;
import org.influxdb.annotation.Measurement;
import java.time.Instant;

@RequiredArgsConstructor
@Data
@Measurement(name = "datapoints", database = "anomaly_detection")
public class DataPoint {
    @Column(name = "sensor") final String sensor;
    @Column(name = "time") final String timestamp;
    @Column(name = "value") final Double value;
    @Column(name = "score") final Double score;
}
