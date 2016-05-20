package cscie55.hw8;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
import java.text.SimpleDateFormat;

public class Hw8Prob2 extends Configured implements Tool {

    public static void main(String args[]) throws Exception {
        int res = ToolRunner.run(new Hw8Prob2(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd-MM-yyyy");
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("EST"));
        Date startDate = simpleDateFormat.parse(args[2]);
        Long startmilliseconds = startDate.getTime()/1000;

        SimpleDateFormat simpleDateFormat2 = new SimpleDateFormat("dd-MM-yyyy");
        simpleDateFormat2.setTimeZone(TimeZone.getTimeZone("EST"));
        Date endDate = simpleDateFormat.parse(args[3]);
        Long endmilliseconds = endDate.getTime()/1000;

        Configuration conf = getConf();

        conf.setLong("startTime",startmilliseconds);
        conf.setLong("endTime",endmilliseconds);

        Job job = new Job(conf, this.getClass().toString());

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setJobName("Hw8Prob2");
        job.setJarByClass(Hw8Prob2.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text value,
                        Mapper.Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            long startmilliseconds = conf.getLong("startTime", 0L);
            long endmilliseconds = conf.getLong("endTime", 0L);

            Link link =  Link.parse(value.toString());
            long jsonTimeStamp = link.timestamp();
            if(jsonTimeStamp > startmilliseconds && jsonTimeStamp < endmilliseconds) {
                word.set(link.url());
                context.write(word, one);
            }

        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }

            context.write(key, new IntWritable(sum));
        }
    }

}
