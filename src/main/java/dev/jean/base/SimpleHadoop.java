package dev.jean.base;

import dev.jean.utils.StringUtils;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public abstract class SimpleHadoop<MAP_KEY_IN, MAP_VALUE_IN, MAP_KEY_OUT, MAP_VALUE_OUT, RED_KEY_OUT, RED_VALUE_OUT>
        extends BaseHadoop<
        MAP_KEY_IN, MAP_VALUE_IN, MAP_KEY_OUT, MAP_VALUE_OUT,
        MAP_KEY_OUT, MAP_VALUE_OUT, MAP_KEY_OUT, MAP_VALUE_OUT,
        MAP_VALUE_IN, MAP_VALUE_OUT, RED_KEY_OUT, RED_VALUE_OUT
        > {

    public SimpleHadoop(String inputFile, Class<MAP_KEY_OUT> mapOutputKeyClass, Class<MAP_VALUE_OUT> mapOutputValueClass, Class<RED_KEY_OUT> outputKeyClass, Class<RED_VALUE_OUT> outputValueClass) {
        this(inputFile, null, mapOutputKeyClass, mapOutputValueClass, outputKeyClass, outputValueClass);
        super.outputFolder = String.format("output/%s", StringUtils.slugify(getClass().getSimpleName()));
    }

    public SimpleHadoop(String inputFile, String outputFolder, Class<MAP_KEY_OUT> mapOutputKeyClass, Class<MAP_VALUE_OUT> mapOutputValueClass, Class<RED_KEY_OUT> outputKeyClass, Class<RED_VALUE_OUT> outputValueClass) {
        super(inputFile, outputFolder, mapOutputKeyClass, mapOutputValueClass, outputKeyClass, outputValueClass);
    }

    @Override
    protected void combine(MAP_KEY_OUT map_key_out, Iterable<MAP_VALUE_OUT> values, Reducer<MAP_KEY_OUT, MAP_VALUE_OUT, MAP_KEY_OUT, MAP_VALUE_OUT>.Context context) throws IOException, InterruptedException {
        for (MAP_VALUE_OUT value : values) {
            context.write(map_key_out, value);
        }
    }
}
