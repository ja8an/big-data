package dev.jean.tde1;

import dev.jean.base.SimpleHadoop;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class Activity7B extends SimpleHadoop<LongWritable, Text, Text, Activity7B.Commodity, Text, DoubleWritable> {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        System.exit((new Activity7B()).run(true) ? 0 : 1);
    }

    public Activity7B() {
        super("output/activity-7/part-r-00000", Text.class, Commodity.class, Text.class, DoubleWritable.class);
        clearOutputFolder();
    }

    @Override
    protected void map(LongWritable longWritable, Text text, Mapper<LongWritable, Text, Text, Commodity>.Context context) throws IOException, InterruptedException {
        String[] split = text.toString().split("\t");
        String[] left = split[0].split("\\.");
        String flow = left[0];
        String commodityCode = left[1];
        double value = Double.parseDouble(split[1]);
        context.write(new Text(flow), new Commodity(String.format("%s-%s", flow, commodityCode), value));
    }

    @Override
    protected void reduce(Text text, Iterable<Commodity> values, Reducer<Text, Commodity, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        String commodity = null;
        double last = 0;
        for (Commodity value : values) {
            if (Objects.isNull(commodity)) {
                commodity = value.commodity;
                last = value.value;
                continue;
            }
            if (value.value > last) {
                commodity = value.commodity;
                last = value.value;
            }
        }
        context.write(new Text(commodity), new DoubleWritable(last));
    }

    @Getter
    @ToString
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Commodity implements Writable {

        private String commodity;
        private double value;

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(commodity);
            out.writeDouble(value);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.commodity = in.readUTF();
            this.value = in.readDouble();
        }
    }
}
