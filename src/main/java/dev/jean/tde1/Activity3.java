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

public class Activity3 extends BaseJob<LongWritable, Text, Text, LongWritable, Text, LongWritable> {
    public Activity3() {
        super(InputFile.TRANSACTIONS, MyMapper.class, MyReducer.class);
    }

    @NoArgsConstructor
    public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private final LongWritable ONE = new LongWritable(1);

        @Override
        protected void map(LongWritable lineNumber, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            if (lineNumber.get() == 0) return;
            Transaction transaction = Transaction.from(value);
            String key = String.format("%d.%s", transaction.getYear(), transaction.getFlow());
            context.write(new Text(key), ONE);
        }
    }

    @NoArgsConstructor
    public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

    public static void main(String[] args) {
        BaseJob.debug(Activity3.class);
    }
}
