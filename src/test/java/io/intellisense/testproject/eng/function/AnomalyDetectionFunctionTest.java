package io.intellisense.testproject.eng.function;


import io.intellisense.testproject.eng.model.DataPoint;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.junit.Assert;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AnomalyDetectionFunctionTest {

    //Mock commons objects
    private Clock mockClock = mock(Clock.class);
    private AnomalyDetectionFunction anomalyDetectionFunction = new AnomalyDetectionFunction(mockClock);;
    private final ProcessAllWindowFunction.Context contextMock = mock(ProcessAllWindowFunction.Context.class);

    @Test
    @DisplayName("Test process")
    void process() throws Exception {

            //Fixture
            Iterator mockIterator = mock(Iterator.class);
             Tuple10<Double, Double, Double, Double, Double, Double, Double, Double, Double, Double> mockTuple10_1 = mock(Tuple10.class);
                mockTuple10_1.f0 = 18.0d;
                mockTuple10_1.f1 = 37.0d;
                mockTuple10_1.f2 = 67.0d;
                mockTuple10_1.f3 = 64.0d;
                mockTuple10_1.f4 = 412.0d;
                mockTuple10_1.f5 = 62.0d;
                mockTuple10_1.f6 = 712.0d;
                mockTuple10_1.f7 = 811.0d;
                mockTuple10_1.f8 = 96.0d;
                mockTuple10_1.f9 = 106.0d;
             Tuple10<Double, Double, Double, Double, Double, Double, Double, Double, Double, Double> mockTuple10_2 = mock(Tuple10.class);
                mockTuple10_2.f0 = 19.0d;
                mockTuple10_2.f1 = 21.0d;
                mockTuple10_2.f2 = 32.0d;
                mockTuple10_2.f3 = 432.0d;
                mockTuple10_2.f4 = 52.0d;
                mockTuple10_2.f5 = 63.0d;
                mockTuple10_2.f6 = 74.0d;
                mockTuple10_2.f7 = 85.0d;
                mockTuple10_2.f8 = 92.0d;
                mockTuple10_2.f9 = 101.0d;
            Tuple10<Double, Double, Double, Double, Double, Double, Double, Double, Double, Double> mockTuple10_3 = mock(Tuple10.class);
                mockTuple10_3.f0 = 12.0d;
                mockTuple10_3.f1 = 26.0d;
                mockTuple10_3.f2 = 13.0d;
                mockTuple10_3.f3 = 44.0d;
                mockTuple10_3.f4 = 45.0d;
                mockTuple10_3.f5 = 67.0d;
                mockTuple10_3.f6 = 77.0d;
                mockTuple10_3.f7 = 18.0d;
                mockTuple10_3.f8 = 94.0d;
                mockTuple10_3.f9 = 109.0d;
            Tuple10<Double, Double, Double, Double, Double, Double, Double, Double, Double, Double> mockTuple10_4 = mock(Tuple10.class);
                mockTuple10_4.f0 = 17.0d;
                mockTuple10_4.f1 = 78.0d;
                mockTuple10_4.f2 = 19.0d;
                mockTuple10_4.f3 = 34.0d;
                mockTuple10_4.f4 = 987.0d;
                mockTuple10_4.f5 = 656.0d;
                mockTuple10_4.f6 = 45.0d;
                mockTuple10_4.f7 = 154.0d;
                mockTuple10_4.f8 = 34.0d;
                mockTuple10_4.f9 = 10.0d;
             Iterable<Tuple10<Double, Double, Double, Double, Double, Double, Double, Double, Double, Double>> dataMock = mock(Iterable.class);

            when(dataMock.iterator()).thenReturn(mockIterator);
            when(mockIterator.next()).thenReturn(mockTuple10_1).thenReturn(mockTuple10_1).thenReturn(mockTuple10_2).thenReturn(mockTuple10_3).thenReturn(mockTuple10_4);
            when(mockTuple10_1.getArity()).thenReturn(10);
            when(dataMock.iterator().hasNext()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);

            Instant mockInstant = mock(Instant.class);
            when(mockClock.instant()).thenReturn(mockInstant);
            when(mockInstant.toString()).thenReturn("mock timestamp");

            //Experimentation
            List<DataPoint> dataPointListReturned = new ArrayList<>();
            ListCollector<DataPoint> listCollectorReturned = new ListCollector<>(dataPointListReturned);
            anomalyDetectionFunction.process(contextMock, dataMock, listCollectorReturned);


            //Expectation
            List<DataPoint> expected = new ArrayList<>();
            expected.add(new DataPoint("Sensor-1", "mock timestamp", 12.0d, 1.0d));
            expected.add(new DataPoint("Sensor-1", "mock timestamp", 17.0d,  1.0d));
            expected.add(new DataPoint("Sensor-1", "mock timestamp", 18.0d, 1.0d));
            expected.add(new DataPoint("Sensor-1", "mock timestamp", 19.0d,  1.0d));
            expected.add(new DataPoint("Sensor-2", "mock timestamp", 21.0d,  0.0d));
            expected.add(new DataPoint("Sensor-2", "mock timestamp", 26.0d,  0.0d));
            expected.add(new DataPoint("Sensor-2", "mock timestamp", 37.0d,  0.0d));
            expected.add(new DataPoint("Sensor-2", "mock timestamp", 78.0d,  0.5d));
            expected.add(new DataPoint("Sensor-3", "mock timestamp", 13.0d,  0.0d));
            expected.add(new DataPoint("Sensor-3", "mock timestamp", 19.0d,  0.0d));
            expected.add(new DataPoint("Sensor-3", "mock timestamp", 32.0d,  0.0d));
            expected.add(new DataPoint("Sensor-3", "mock timestamp", 67.0d,  0.0d));
            expected.add(new DataPoint("Sensor-4", "mock timestamp", 34.0d,  0.0d));
            expected.add(new DataPoint("Sensor-4", "mock timestamp", 44.0d,  0.0d));
            expected.add(new DataPoint("Sensor-4", "mock timestamp", 64.0d,  0.0d));
            expected.add(new DataPoint("Sensor-4", "mock timestamp", 432.0d,  0.0d));
            expected.add(new DataPoint("Sensor-5", "mock timestamp", 45.0d,  0.0d));
            expected.add(new DataPoint("Sensor-5", "mock timestamp", 52.0d,  0.0d));
            expected.add(new DataPoint("Sensor-5", "mock timestamp", 412.0d,  0.0d));
            expected.add(new DataPoint("Sensor-5", "mock timestamp", 987.0d,  0.0d) );
            expected.add(new DataPoint("Sensor-6", "mock timestamp",  62.0d, 0.0d));
            expected.add(new DataPoint("Sensor-6", "mock timestamp",  63.0d, 0.0d));
            expected.add(new DataPoint("Sensor-6", "mock timestamp",  67.0d, 0.0d));
            expected.add(new DataPoint("Sensor-6", "mock timestamp",  656.0d, 0.0d));
            expected.add(new DataPoint("Sensor-7", "mock timestamp",  45.0d, 0.0d));
            expected.add(new DataPoint("Sensor-7", "mock timestamp",  74.0d, 0.0d));
            expected.add(new DataPoint("Sensor-7", "mock timestamp",  77.0d, 0.0d));
            expected.add(new DataPoint("Sensor-7", "mock timestamp",  712.0d, 0.0d));
            expected.add(new DataPoint("Sensor-8", "mock timestamp",  18.0d, 0.0d));
            expected.add(new DataPoint("Sensor-8", "mock timestamp",  85.0d, 0.0d));
            expected.add(new DataPoint("Sensor-8", "mock timestamp",  154.0d, 0.0d));
            expected.add(new DataPoint("Sensor-8", "mock timestamp",  811.0d, 0.0d));
            expected.add(new DataPoint("Sensor-9", "mock timestamp",  34.0d, 1.0d));
            expected.add(new DataPoint("Sensor-9", "mock timestamp",  92.0d, 1.0d));
            expected.add(new DataPoint("Sensor-9", "mock timestamp",  94.0d, 1.0d));
            expected.add(new DataPoint("Sensor-9", "mock timestamp",  96.0d, 1.0d));
            expected.add(new DataPoint("Sensor-10", "mock timestamp",  10.0d, 0.0d));
            expected.add(new DataPoint("Sensor-10", "mock timestamp",  101.0d, 1.0d));
            expected.add(new DataPoint("Sensor-10", "mock timestamp",  106.0d, 1.0d));
            expected.add(new DataPoint("Sensor-10", "mock timestamp",  109.0d, 1.0d));


         Assert.assertEquals(dataPointListReturned, expected);

    }

}
