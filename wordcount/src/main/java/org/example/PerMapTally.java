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


public class PerMapTally {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {
        private Text word = new Text();
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            HashMap<Text, IntWritable> wordMap = new HashMap<Text, IntWritable>();
            while (itr.hasMoreTokens()) {
                String curr = itr.nextToken();
                int first = curr.charAt(0);
                if (first == 'm' || first == 'n' || first == 'o' || first == 'p' || first == 'q' ||
                        first == 'M' || first == 'N' || first == 'O' || first == 'P' || first == 'Q') {
                    word = new Text(curr);
                    if (wordMap.containsKey(word)) {
                        IntWritable count = wordMap.get(word);
                        count.set(count.get() + 1);
                        wordMap.put(word, count);
                    } else {
                        wordMap.put(word, new IntWritable(1));
                    }
                }
            }

            for (Map.Entry<Text, IntWritable> w : wordMap.entrySet()) {
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
        job.setJarByClass(PerMapTally.class);
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