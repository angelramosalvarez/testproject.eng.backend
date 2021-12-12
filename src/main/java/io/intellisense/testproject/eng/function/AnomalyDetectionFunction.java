package io.intellisense.testproject.eng.function;

import io.intellisense.testproject.eng.model.DataPoint;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import java.time.Clock;
import java.util.*;


public class AnomalyDetectionFunction extends ProcessAllWindowFunction<Tuple10<Double, Double, Double, Double, Double, Double, Double, Double, Double, Double>, DataPoint, GlobalWindow> {


    private final Clock clock;
    public AnomalyDetectionFunction(Clock clock) {
        this.clock = clock;
    }


    @Override
    public void process(Context context, Iterable<Tuple10<Double, Double, Double, Double, Double, Double, Double, Double, Double, Double>> data, Collector<DataPoint> out) throws Exception {

        //1. -Get the number of sensors
        int numberOfSensors = data.iterator().next().getArity();

        //2.- Transpose data for each sensor
        List<List<Double>> listsOfSensors = transposeDataSensors(data, numberOfSensors);

        //3.- Order sensor lists
        for (int i = 0; i < numberOfSensors; i++) {
            Collections.sort(listsOfSensors.get(i));
        }

        //4.- Calculate anomalous score for each sensor data
        for (int i = 0; i < numberOfSensors; i++) {

            //Get sensor-i data list
            List<Double> sensorDataList = listsOfSensors.get(i);

            //Get IQR
            double iqr = calculateIQR(sensorDataList);

            //Sensor number
            int sensorNumber = i + 1;

            //Calculate data point with Sensor-i, timeStamp, value and anomalousScore
            for (int d = 0; d < sensorDataList.size(); d++) {

                //Get value d
                double value = sensorDataList.get(d);

                //Calculate anomalous score
                double anomalousScore = calculateAnomalousScore(value, iqr);

                //Create a new DataPoint
                //String timestamp = Instant.now().toString();
                String timestamp = clock.instant().toString();
                DataPoint dataPoint = new DataPoint("Sensor-" + sensorNumber, timestamp, value, anomalousScore);
                out.collect(dataPoint);
            }
        }

    }


    //Transpose data for each sensor
    private List<List<Double>> transposeDataSensors(Iterable<Tuple10<Double, Double, Double, Double, Double, Double, Double, Double, Double, Double>> data, int numberOfSensors){

        //Create a list for each sensor in order to transpose the data
        List<List<Double>> listsOfSensors = new ArrayList<List<Double>>();
        for (int i = 0; i < numberOfSensors; i++) {
            List<Double> dataOfSensor = new ArrayList<Double>();
            listsOfSensors.add(dataOfSensor);
        }

        //Transpose data for each sensor
        for (Tuple10<Double, Double, Double, Double, Double, Double, Double, Double, Double, Double> in : data) {
            int index = 0;
            listsOfSensors.get(index).add(in.f0);
            listsOfSensors.get(++index).add(in.f1);
            listsOfSensors.get(++index).add(in.f2);
            listsOfSensors.get(++index).add(in.f3);
            listsOfSensors.get(++index).add(in.f4);
            listsOfSensors.get(++index).add(in.f5);
            listsOfSensors.get(++index).add(in.f6);
            listsOfSensors.get(++index).add(in.f7);
            listsOfSensors.get(++index).add(in.f8);
            listsOfSensors.get(++index).add(in.f9);
        }

        return listsOfSensors;
    }

    // Calculate anomalous score according next algorithm:
    //  1. Calculate the interquartile range (IQR) for the array of data sensor
    //  2. Based on the IQR, score the value being processed with the following:
    //      a. If the value is < 1.5 * IQR, assign 0
    //      b. If it is >= 1.5 * IQR and < 3 * IQR, assign 0.5
    //      c. If it is >= 3 * IQR, assign 1
    private double calculateAnomalousScore(double value, double iqr){

        double anomalousScore = 0.0;
        double iqrMin = 1.5*iqr;
        double iqrMax = 3*iqr;

        if (value < iqrMin) anomalousScore = 0.0;
        else if (value >= iqrMin && value < iqrMax) anomalousScore = 0.5;
        else if (value >= iqrMax) anomalousScore = 1.0;

        return anomalousScore;

    }

    // calculate IQR
    private double calculateIQR(List<Double> sensorDataList) {

        //size of the data list
        int size = sensorDataList.size();

        // Calculate index of medians
        int midLowerHalf = (size + 1) / 4;
        int midUpperHalf = (3 * (size + 1)) / 4;

        // get quartiles Q1 and Q3
        double q1 = sensorDataList.get(midLowerHalf);
        double q3 = sensorDataList.get(midUpperHalf);

        // IQR calculation
        return (q3 - q1);
    }



}