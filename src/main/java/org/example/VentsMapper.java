package org.example;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static java.lang.Double.parseDouble;

public class VentsMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        String[] obj = value.toString().split(" ");
        String ville = obj[1];
        double prix  = Double.parseDouble(obj[3]);
        context.write(new Text(ville), new DoubleWritable(prix));
    }
}
