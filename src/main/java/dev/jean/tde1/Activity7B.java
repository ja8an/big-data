package dev.jean.tde1;

import dev.jean.BaseJob;
import dev.jean.utils.InputFile;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class Activity7B extends BaseJob<LongWritable, Text, Text, Activity7B.MyWritable, Text, Activity7B.MyWritable> {


    public Activity7B() {
        super("output/activity-7/part-r-00000", MyMapper.class, MyReducer.class);
    }

    @NoArgsConstructor
    public static class MyMapper extends Mapper<LongWritable, Text, Text, MyWritable> {
        @Override
        protected void map(LongWritable lineNumber, Text value, Mapper<LongWritable, Text, Text, MyWritable>.Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            String[] cols = line[0].split("\\.");
            Double sum = Double.parseDouble(line[1]);
            String commodityCode = cols[0];
            String flow = cols[1];
            context.write(new Text(flow), new MyWritable(commodityCode, sum));
        }
    }

    @NoArgsConstructor
    public static class MyReducer extends Reducer<Text, MyWritable, Text, MyWritable> {
        @Override
        protected void reduce(Text key, Iterable<MyWritable> values, Reducer<Text, MyWritable, Text, MyWritable>.Context context) throws IOException, InterruptedException {
            MyWritable max = null;
            for (MyWritable value : values) {
                if (Objects.isNull(max)) max = value;
                if (value.sum > max.sum) {
                    max = value;
                }
            }
            context.write(key, max);
        }
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @ToString
    public static class MyWritable implements Writable {
        private String commodityCode;
        private Double sum;

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(commodityCode);
            out.writeDouble(sum);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.commodityCode = in.readUTF();
            this.sum = in.readDouble();
        }
    }

    public static void main(String[] args) {
        BaseJob.debug(Activity7B.class);
    }
}
