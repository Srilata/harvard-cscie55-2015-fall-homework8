package cscie55.hw8;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;

public class Hw8Prob1 extends Configured implements Tool {


    public static void main(String args[]) throws Exception {
        int res = ToolRunner.run(new Hw8Prob1(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        Configuration conf = getConf();
        Job job = new Job(conf, this.getClass().toString());

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setJobName("Hw8Prob1");
        job.setJarByClass(Hw8Prob1.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private Text word = new Text();


        @Override
        public void map(LongWritable key, Text value,
                        Context context) throws IOException, InterruptedException {
            Link link =  Link.parse(value.toString());
            word.set(link.url());
            context.write(word, new Text(link.tagString()) );
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            java.util.Set<String> tags = new HashSet<String>();
            for (Text value : values) {
                String str = value.toString();
                StringTokenizer st = new StringTokenizer(str, ":");
                if (st.hasMoreTokens()) {
                    String tag = st.nextToken();
                    tags.add(tag);

                }
            }

            context.write(key, new Text(tags.toString().replace("[", "").replace("]", "")
                    .replace(", ", ",")));
        }
    }

}
