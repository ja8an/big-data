package dev.jean.base;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.File;
import java.io.IOException;

/*
 * SÃ³ funciona com um reducer local
 * */
public abstract class BaseHadoop<MAP_KEY_IN, MAP_VALUE_IN, MAP_KEY_OUT, MAP_VALUE_OUT,
        COM_KEY_IN, COM_VALUE_IN, COM_KEY_OUT, COM_VALUE_OUT,
        RED_KEY_IN, RED_VALUE_IN, RED_KEY_OUT, RED_VALUE_OUT> {

    private static BaseHadoop<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?> objects;
    private String jobName;
    private final String inputFile;
    protected String outputFolder;
    private boolean clearOutputFolder = false;

    private final Class<MAP_KEY_OUT> mapOutputKeyClass;
    private final Class<MAP_VALUE_OUT> mapOutputValueClass;
    private final Class<RED_KEY_OUT> outputKeyClass;
    private final Class<RED_VALUE_OUT> outputValueClass;

    private final BaseHadoop<MAP_KEY_IN, MAP_VALUE_IN, MAP_KEY_OUT, MAP_VALUE_OUT,
            COM_KEY_IN, COM_VALUE_IN, COM_KEY_OUT, COM_VALUE_OUT,
            RED_KEY_IN, RED_VALUE_IN, RED_KEY_OUT, RED_VALUE_OUT> self = this;

    public BaseHadoop(String inputFile, Class<MAP_KEY_OUT> mapOutputKeyClass, Class<MAP_VALUE_OUT> mapOutputValueClass, Class<RED_KEY_OUT> outputKeyClass, Class<RED_VALUE_OUT> outputValueClass) {
        this(inputFile, "output/data", mapOutputKeyClass, mapOutputValueClass, outputKeyClass, outputValueClass);
    }

    public BaseHadoop(String inputFile, String outputFolder, Class<MAP_KEY_OUT> mapOutputKeyClass, Class<MAP_VALUE_OUT> mapOutputValueClass, Class<RED_KEY_OUT> outputKeyClass, Class<RED_VALUE_OUT> outputValueClass) {
        this.inputFile = inputFile;
        this.outputFolder = outputFolder;

        this.mapOutputKeyClass = mapOutputKeyClass;
        this.mapOutputValueClass = mapOutputValueClass;
        this.outputKeyClass = outputKeyClass;
        this.outputValueClass = outputValueClass;

        this.jobName = getClass().getSimpleName();
    }

    public void jobName(String jobName) {
        this.jobName = jobName;
    }

    public void clearOutputFolder() {
        this.clearOutputFolder = true;
    }

    protected abstract void map(MAP_KEY_IN key, MAP_VALUE_IN value, Mapper<MAP_KEY_IN, MAP_VALUE_IN, MAP_KEY_OUT, MAP_VALUE_OUT>.Context context) throws IOException, InterruptedException;

    abstract void combine(COM_KEY_IN key, Iterable<COM_VALUE_IN> values, Reducer<COM_KEY_IN, COM_VALUE_IN, COM_KEY_OUT, COM_VALUE_OUT>.Context context) throws IOException, InterruptedException;

    protected abstract void reduce(RED_KEY_IN key, Iterable<RED_VALUE_IN> values, Reducer<RED_KEY_IN, RED_VALUE_IN, RED_KEY_OUT, RED_VALUE_OUT>.Context context) throws IOException, InterruptedException;

    public boolean run() throws IOException, InterruptedException, ClassNotFoundException {
        return run(false);
    }

    public boolean run(boolean verbose) throws IOException, InterruptedException, ClassNotFoundException {
        BasicConfigurator.configure();

        Configuration config = new Configuration();

        // arquivo de entrada
        Path input = new Path(inputFile);

        // arquivo de saida
        Path output = new Path(outputFolder);

        // criacao do job e seu nome
        Job job = Job.getInstance(config, jobName);

        // registro das classes
        job.setJarByClass(getClass());
        job.setMapperClass(BaseMapper.class);
        // definicao dos tipos de saida
        job.setMapOutputKeyClass(mapOutputKeyClass);
        job.setMapOutputValueClass(mapOutputValueClass);

        job.setCombinerClass(BaseCombiner.class);

        job.setReducerClass(BaseReducer.class);
        job.setOutputKeyClass(outputKeyClass);
        job.setOutputValueClass(outputValueClass);

        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        if (clearOutputFolder) {
            FileUtil.fullyDelete(new File(outputFolder));
        }
        objects = this;
        // lanca o job e aguarda sua execucao
        return job.waitForCompletion(verbose);
    }

    static class BaseMapper<MAP_KEY_IN, MAP_VALUE_IN, MAP_KEY_OUT, MAP_VALUE_OUT> extends Mapper<MAP_KEY_IN, MAP_VALUE_IN, MAP_KEY_OUT, MAP_VALUE_OUT> {
        public BaseMapper() {
            super();
        }

        @Override
        @SuppressWarnings("unchecked")
        public void map(MAP_KEY_IN key, MAP_VALUE_IN value, Context context) throws IOException, InterruptedException {
            BaseHadoop<MAP_KEY_IN, MAP_VALUE_IN, MAP_KEY_OUT, MAP_VALUE_OUT, Object, Object, Object, Object, Object, Object, Object, Object> mapper = (BaseHadoop<MAP_KEY_IN, MAP_VALUE_IN, MAP_KEY_OUT, MAP_VALUE_OUT, Object, Object, Object, Object, Object, Object, Object, Object>) objects;
            mapper.map(key, value, context);
        }
    }

    static class BaseReducer<RED_KEY_IN, RED_VALUE_IN, RED_KEY_OUT, RED_VALUE_OUT> extends Reducer<RED_KEY_IN, RED_VALUE_IN, RED_KEY_OUT, RED_VALUE_OUT> {
        public BaseReducer() {
        }

        @Override
        @SuppressWarnings("unchecked")
        protected void reduce(RED_KEY_IN key, Iterable<RED_VALUE_IN> values, Reducer<RED_KEY_IN, RED_VALUE_IN, RED_KEY_OUT, RED_VALUE_OUT>.Context context) throws IOException, InterruptedException {
            BaseHadoop<Object, Object, Object, Object, Object, Object, Object, Object, RED_KEY_IN, RED_VALUE_IN, RED_KEY_OUT, RED_VALUE_OUT> reducer = (BaseHadoop<Object, Object, Object, Object, Object, Object, Object, Object, RED_KEY_IN, RED_VALUE_IN, RED_KEY_OUT, RED_VALUE_OUT>) objects;
            reducer.reduce(key, values, context);
        }
    }

    static class BaseCombiner<COM_KEY_IN, COM_VALUE_IN, COM_KEY_OUT, COM_VALUE_OUT> extends Reducer<COM_KEY_IN, COM_VALUE_IN, COM_KEY_OUT, COM_VALUE_OUT> {
        public BaseCombiner() {
        }

        @Override
        @SuppressWarnings("unchecked")
        protected void reduce(COM_KEY_IN key, Iterable<COM_VALUE_IN> values, Reducer<COM_KEY_IN, COM_VALUE_IN, COM_KEY_OUT, COM_VALUE_OUT>.Context context) throws IOException, InterruptedException {
            BaseHadoop<Object, Object, Object, Object, COM_KEY_IN, COM_VALUE_IN, COM_KEY_OUT, COM_VALUE_OUT, Object, Object, Object, Object> reducer = (BaseHadoop<Object, Object, Object, Object, COM_KEY_IN, COM_VALUE_IN, COM_KEY_OUT, COM_VALUE_OUT, Object, Object, Object, Object>) objects;
            reducer.combine(key, values, context);
        }
    }

}
