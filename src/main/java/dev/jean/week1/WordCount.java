package dev.jean.week1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.text.Normalizer;


public class WordCount {

    public static void main(final String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();


        // job instance
        Job job = Job.getInstance(c, "wordcount");

        // class register
        job.setJarByClass(WordCount.class);
        job.setMapperClass(MapForWordCount.class);
        job.setReducerClass(ReduceForWordCount.class);

        // output type definition
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // file definition
        // input file
        Path input = new Path(files[0]);
        FileInputFormat.addInputPath(job, input);
        // output file
        Path output = new Path(files[1]);
        FileOutputFormat.setOutputPath(job, output);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

    static String prepareWord(final String input) {
        return Normalizer.normalize(input, Normalizer.Form.NFKD)
                .replaceAll("\\p{M}", "")
                .toLowerCase();
    }

    public static class MapForWordCount extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(final LongWritable key, final Text value, final Context con)
                throws IOException, InterruptedException {
            String[] words = value.toString().split("([^\\w\\-']+)");
            for (String word : words) {
                if (word.length() == 0) continue;
                word = prepareWord(word);
                con.write(new Text(word), new IntWritable(1));
            }
        }
    }

    public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(final Text key, final Iterable<IntWritable> values, final Context con)
                throws IOException, InterruptedException {
            int soma = 0;
            for (IntWritable value : values) {
                soma += value.get();
            }
            con.write(key, new IntWritable(soma));
        }
    }

}
