package org.example;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.w3c.dom.Text;

import java.io.IOException;
import java.util.Iterator;

public class VentsReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        Iterator<DoubleWritable> prices= values.iterator();
        double prix = 0;
        while (prices.hasNext()){
            prix += prices.next().get();
        }
        context.write(key, new DoubleWritable(prix));
    }
}
