package dev.jean.tde1;

import dev.jean.BaseJob;
import dev.jean.utils.InputFile;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Activity2 extends BaseJob<LongWritable, Text, IntWritable, IntWritable, IntWritable, IntWritable> {


    public Activity2() {
        super(InputFile.TRANSACTIONS, MyMapper.class, MyReducer.class);
    }

    @NoArgsConstructor
    public static class MyMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

        private final IntWritable ONE = new IntWritable(1);

        @Override
        protected void map(LongWritable lineNumber, Text value, Mapper<LongWritable, Text, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {
            if (lineNumber.get() == 0) return;
            Transaction transaction = Transaction.from(value);
            context.write(new IntWritable(transaction.getYear()), ONE);
        }
    }

    @NoArgsConstructor
    public static class MyReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) {
        System.exit(
                BaseJob.debug(Activity2.class)
        );
    }
}
