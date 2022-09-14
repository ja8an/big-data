package dev.jean.tde1;

import dev.jean.BaseJob;
import dev.jean.utils.InputFile;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Activity7 extends BaseJob<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> {
    public Activity7() {
        super(InputFile.TRANSACTIONS, MyMapper.class, MyReducer.class);
    }

    @NoArgsConstructor
    public static class MyMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        @Override
        protected void map(LongWritable lineNumber, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
            if (lineNumber.get() == 0) return;
            Transaction transaction = Transaction.from(value);
            if (transaction.getYear() == 2016) {
                String key = String.format("%s.%s",
                        transaction.getCommodityCode(),
                        transaction.getFlow()
                );
                context.write(new Text(key), new DoubleWritable(transaction.getAmount()));
            }
        }
    }

    @NoArgsConstructor
    public static class MyReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable value : values) {
                sum += value.get();
            }
            context.write(key, new DoubleWritable(sum));
        }
    }

    public static void main(String[] args) {
        BaseJob.debug(Activity7.class);
    }
}
