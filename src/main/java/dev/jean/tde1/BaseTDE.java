package dev.jean.tde1;

import dev.jean.base.SimpleHadoop;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public abstract class BaseTDE<MAP_KEY_OUT, MAP_VALUE_OUT, RED_KEY_OUT, RED_VALUE_OUT>
        extends SimpleHadoop<LongWritable, Text, MAP_KEY_OUT, MAP_VALUE_OUT, RED_KEY_OUT, RED_VALUE_OUT> {

    public BaseTDE(final Class<MAP_KEY_OUT> mapOutputKeyClass, final Class<MAP_VALUE_OUT> mapOutputValueClass,
                   final Class<RED_KEY_OUT> outputKeyClass, final Class<RED_VALUE_OUT> outputValueClass) {
        super("in/transactions_amostra.csv", mapOutputKeyClass, mapOutputValueClass, outputKeyClass, outputValueClass);
        clearOutputFolder();
    }
}
