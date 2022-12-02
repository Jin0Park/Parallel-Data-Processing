package org.hcompute;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.hbase.client.*;


import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;

public class flightMonthlyDelayAvgUsingHBase {
    static class hbaseMapper extends TableMapper<Text, Text> {
        public void map(ImmutableBytesWritable key, Result value, Context context
        ) throws IOException, InterruptedException {
            List<String> keyElements = Arrays.asList(key.toString().split(","));
            String year = keyElements.get(2);
            String isValidFlight = keyElements.get(3);
            if (year.equals("2008") && isValidFlight.equals("1")) {
                Text k = new Text(keyElements.get(1));
                String month = Bytes.toString(value.getValue(Bytes.toBytes("flightData"), Bytes.toBytes("Month")));
                String delayMin = Bytes.toString(value.getValue(Bytes.toBytes("flightData"), Bytes.toBytes("ArrDelayMinutes")));
                Text v = new Text(month + "," + delayMin);
                context.write(k, v);
            }
        }
    }

    public static class hbaseReducer extends TableReducer<Text, Text, Text> {
        double count = 0.00;
        double totalDelay = 0.00;
        private static HashMap<String, List<Double>> monthAVG = new HashMap<String, List<Double>>() {{
            put("1", Arrays.asList(0.00, 0.00));
            put("2", Arrays.asList(0.00, 0.00));
            put("3", Arrays.asList(0.00, 0.00));
            put("4", Arrays.asList(0.00, 0.00));
            put("5", Arrays.asList(0.00, 0.00));
            put("6", Arrays.asList(0.00, 0.00));
            put("7", Arrays.asList(0.00, 0.00));
            put("8", Arrays.asList(0.00, 0.00));
            put("9", Arrays.asList(0.00, 0.00));
            put("10", Arrays.asList(0.00, 0.00));
            put("11", Arrays.asList(0.00, 0.00));
            put("12", Arrays.asList(0.00, 0.00));
        }};

        public void reduce(Text key, Iterable<Text> values, Context context
        ) throws IOException, InterruptedException {
            String month;
            double curr_count;
            double curr_sum;
            for (Text v : values) {
                String strValue = v.toString();
                List<String> listValue = Arrays.asList(strValue.split(","));
                month = listValue.get(0);
                curr_count = monthAVG.get(month).get(0) + 1;
                curr_sum = monthAVG.get(month).get(1) + (Double.valueOf(listValue.get(1)));
                monthAVG.get(month).set(0, curr_count);
                monthAVG.get(month).set(1, curr_sum);
            }
            ArrayList<Put> elements = new ArrayList<Put>();
            Put put = new Put(key.toString().getBytes());

            for (String k : monthAVG.keySet()) {
                Double avg = 0.00;
                if (monthAVG.get(k).get(0) != 0) {
                    avg = monthAVG.get(k).get(1) / monthAVG.get(k).get(0);
                }
                put.addColumn(Bytes.toBytes("flightData"), Bytes.toBytes(k), Bytes.toBytes(avg));
                elements.add(put);
            }
            context.write(key, put);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "flightComputation");
        job.setJarByClass(flightMonthlyDelayAvgUsingHBase.class);

        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);

        TableMapReduceUtil.initTableMapperJob(
                "hpopulate",      // input table
                scan,              // Scan instance to control CF and attribute selection
                hbaseMapper.class,   // mapper class
                Text.class,              // mapper output key
                Text.class,              // mapper output value
                job);
        TableMapReduceUtil.initTableReducerJob(
                "hcompute",      // output table
                hbaseReducer.class,             // reducer class
                job);
        job.setNumReduceTasks(0);

        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException();
        }
    }
}
