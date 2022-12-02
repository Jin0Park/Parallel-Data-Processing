package org.example;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


public class PerTaskTally {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {
        private HashMap<Text, IntWritable> collectedWords = new HashMap<Text, IntWritable>();
        //private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String curr = itr.nextToken();
                int first = curr.charAt(0);
                if (first == 'm' || first == 'n' || first == 'o' || first == 'p' || first == 'q' ||
                        first == 'M' || first == 'N' || first == 'O' || first == 'P' || first == 'Q') {
                    Text word = new Text(curr);
                    if (collectedWords.containsKey(word)) {
                        IntWritable count = collectedWords.get(word);
                        count.set(count.get() + 1);
                        collectedWords.put(word, count);
                    } else {
                        collectedWords.put(word, new IntWritable(1));
                    }
                }
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Text, IntWritable> w : collectedWords.entrySet()) {
                context.write(w.getKey(), w.getValue());
            }
        }

    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    // Custom partitioner
    public static class CustomPartitioner extends Partitioner<Text,IntWritable>{
        @Override
        public int getPartition(Text text, IntWritable intWritable, int i) {
            String partitionKey = text.toString().toLowerCase();
            if (partitionKey.startsWith("m")) {
                return 0;
            } else if (partitionKey.startsWith("n")) {
                return 1;
            } else if (partitionKey.startsWith("o")) {
                return 2;
            } else if (partitionKey.startsWith("p")) {
                return 3;
            } else if (partitionKey.startsWith("q")) {
                return 4;
            }
            return -1;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "wordCount");
        job.setJarByClass(PerTaskTally.class);
        job.setMapperClass(TokenizerMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setPartitionerClass(CustomPartitioner.class);
        job.setNumReduceTasks(5);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}