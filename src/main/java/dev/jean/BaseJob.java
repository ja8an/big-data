package dev.jean;

import dev.jean.utils.InputFile;
import dev.jean.utils.StringUtils;
import lombok.SneakyThrows;
import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.BasicConfigurator;

import java.io.File;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

public abstract class BaseJob<
        MAP_KEY_IN extends Writable, MAP_VAL_IN extends Writable,
        RED_KEY_IN extends Writable, RED_VAL_IN extends Writable, RED_KEY_OUT extends Writable, RED_VAL_OUT extends Writable
        > {

    private final Job job;
    private final String outputFolder;

    public BaseJob(InputFile inputFile,
                   Class<? extends Mapper<MAP_KEY_IN, MAP_VAL_IN, RED_KEY_IN, RED_VAL_IN>> mapperClass,
                   Class<? extends Reducer<RED_KEY_IN, RED_VAL_IN, RED_KEY_OUT, RED_VAL_OUT>> reducerClass) {
        this(inputFile.getPath(), mapperClass, reducerClass);
    }

    @SneakyThrows
    public BaseJob(String inputFile,
                   Class<? extends Mapper<MAP_KEY_IN, MAP_VAL_IN, RED_KEY_IN, RED_VAL_IN>> mapperClass,
                   Class<? extends Reducer<RED_KEY_IN, RED_VAL_IN, RED_KEY_OUT, RED_VAL_OUT>> reducerClass
    ) {
        Configuration conf = new Configuration();

        Class<? extends BaseJob> baseClass = getClass();
        String jobName = baseClass.getSimpleName();

        this.job = Job.getInstance(conf, jobName);

        job.setJarByClass(getClass());

        Type[] types = ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments();

        // Setup mapper
        job.setMapperClass(mapperClass);
        Class<RED_KEY_IN> redKeyInClass = (Class<RED_KEY_IN>) types[2];
        job.setMapOutputKeyClass(redKeyInClass);
        Class<RED_VAL_IN> redValInClass = (Class<RED_VAL_IN>) types[3];
        job.setMapOutputValueClass(redValInClass);

        // Setup reducer
        job.setReducerClass(reducerClass);
        Class<RED_KEY_OUT> redKeyOutClass = (Class<RED_KEY_OUT>) types[2];
        job.setOutputKeyClass(redKeyOutClass);
        Class<RED_VAL_OUT> redValOutClass = (Class<RED_VAL_OUT>) types[2];
        job.setOutputValueClass(redValOutClass);

        FileInputFormat.addInputPath(job, new Path(inputFile));
        this.outputFolder = String.format("output/%s", StringUtils.slugify(jobName));
        FileOutputFormat.setOutputPath(job, new Path(outputFolder));

    }

    public final boolean run() {
        return run(false);
    }

    @SneakyThrows
    public final boolean run(boolean verbose) {
        FileUtil.fullyDelete(new File(outputFolder));
        return job.waitForCompletion(verbose);
    }

    @SneakyThrows
    public static int debug(Class<? extends BaseJob<?, ?, ?, ?, ?, ?>> baseClass) {
        BaseJob baseJob = baseClass.getConstructor().newInstance();
        BasicConfigurator.configure();
        boolean success = false;
        try {
            success = baseJob.run(true);
        } catch (Exception exception) {
            exception.printStackTrace();
        }
        BasicConfigurator.resetConfiguration();
        return success ? 0 : 1;
    }

}
