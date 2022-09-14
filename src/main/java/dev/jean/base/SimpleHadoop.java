package dev.jean.base;

import dev.jean.utils.StringUtils;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public abstract class SimpleHadoop<MAP_KEY_IN, MAP_VALUE_IN, MAP_KEY_OUT, MAP_VALUE_OUT, RED_KEY_OUT, RED_VALUE_OUT>
        extends BaseHadoop<
        MAP_KEY_IN, MAP_VALUE_IN, MAP_KEY_OUT, MAP_VALUE_OUT,
        MAP_KEY_OUT, MAP_VALUE_OUT, MAP_KEY_OUT, MAP_VALUE_OUT,
        MAP_KEY_OUT, MAP_VALUE_OUT, RED_KEY_OUT, RED_VALUE_OUT
        > {

    public SimpleHadoop(final String inputFile,
                        final Class<MAP_KEY_OUT> mapOutputKeyClass, final Class<MAP_VALUE_OUT> mapOutputValueClass,
                        final Class<RED_KEY_OUT> outputKeyClass, final Class<RED_VALUE_OUT> outputValueClass) {
        this(inputFile, null, mapOutputKeyClass, mapOutputValueClass, outputKeyClass, outputValueClass);
        outputFolder(String.format("output/%s", StringUtils.slugify(getClass().getSimpleName())));
    }

    public SimpleHadoop(final String inputFile, final String outputFolder,
                        final Class<MAP_KEY_OUT> mapOutputKeyClass, final Class<MAP_VALUE_OUT> mapOutputValueClass,
                        final Class<RED_KEY_OUT> outputKeyClass, final Class<RED_VALUE_OUT> outputValueClass) {
        super(inputFile, outputFolder, mapOutputKeyClass, mapOutputValueClass, outputKeyClass, outputValueClass);
    }

    @Override
    void combine(final MAP_KEY_OUT mapKeyOut, final Iterable<MAP_VALUE_OUT> values,
                 final Reducer<MAP_KEY_OUT, MAP_VALUE_OUT, MAP_KEY_OUT, MAP_VALUE_OUT>.Context context) throws IOException, InterruptedException {
        for (MAP_VALUE_OUT value : values) {
            context.write(mapKeyOut, value);
        }
    }
}
