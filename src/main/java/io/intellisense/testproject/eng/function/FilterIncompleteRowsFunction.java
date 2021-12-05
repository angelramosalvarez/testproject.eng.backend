package io.intellisense.testproject.eng.function;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.types.Row;

public class FilterIncompleteRowsFunction implements FilterFunction<Row> {

    //filter the row if has some columns is empty value
    @Override
    public boolean filter(Row value) throws Exception {
        boolean ret = true;
        int index = 0;
        while(ret && index < value.getArity()){
            ret = ret && !value.getField(index).equals(" ");
            index++;
        }
       return ret;
    }

}
