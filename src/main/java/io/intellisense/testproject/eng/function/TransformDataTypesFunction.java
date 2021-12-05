package io.intellisense.testproject.eng.function;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.types.Row;

public class TransformDataTypesFunction implements MapFunction<Row, Tuple10<Double, Double, Double, Double, Double, Double, Double, Double, Double, Double>> {

    @Override
    public Tuple10<Double, Double, Double, Double, Double, Double, Double, Double, Double, Double> map(Row row) throws Exception {

        return new Tuple10<>(
                Double.parseDouble(row.getField(1).toString()),
                Double.parseDouble(row.getField(2).toString()),
                Double.parseDouble(row.getField(3).toString()),
                Double.parseDouble(row.getField(4).toString()),
                Double.parseDouble(row.getField(5).toString()),
                Double.parseDouble(row.getField(6).toString()),
                Double.parseDouble(row.getField(7).toString()),
                Double.parseDouble(row.getField(8).toString()),
                Double.parseDouble(row.getField(9).toString()),
                Double.parseDouble(row.getField(10).toString()));
    }


}
