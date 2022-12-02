package org.example;

import net.minidev.json.JSONUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.*;


public class flightMonthlyDelayAvg {
    public flightMonthlyDelayAvg() {

    }

    private static List<String> headerNames = Arrays.asList(
            "Year","Quarter","Month", "DayofMonth","DayOfWeek","FlightDate","UniqueCarrier","AirlineID",
            "Carrier","TailNum","FlightNum","Origin","OriginCityName","OriginState","OriginStateFips",
            "OriginStateName","OriginWac","Dest","DestCityName","DestState","DestStateFips","DestStateName",
            "DestWac","CRSDepTime","DepTime", "DepDelay","DepDelayMinutes","DepDel15","DepartureDelayGroups",
            "DepTimeBlk","TaxiOut","WheelsOff","WheelsOn","TaxiIn","CRSArrTime", "ArrTime","ArrDelay",
            "ArrDelayMinutes","ArrDel15","ArrivalDelayGroups","ArrTimeBlk","Cancelled","CancellationCode",
            "Diverted","CRSElapsedTime","ActualElapsedTime","AirTime","Flights","Distance","DistanceGroup",
            "CarrierDelay","WeatherDelay","NASDelay","SecurityDelay","LateAircraftDelay");
    private static HashMap<String, Integer> neededColumnsIndices = new HashMap<String, Integer>(){{
        put("Year", headerNames.indexOf("Year"));
        put("Month", headerNames.indexOf("Month"));
        put("ArrDelayMinutes", headerNames.indexOf("ArrDelayMinutes"));
        put("AirlineID", headerNames.indexOf("AirlineID"));
        put("Cancelled", headerNames.indexOf("Cancelled"));
        put("Diverted", headerNames.indexOf("Diverted"));
    }};

    private static List<String> neededColumns = Arrays.asList("Year", "Month", "AirlineID", "ArrDelayMinutes", "Cancelled", "Diverted");

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
    public static class TokenizerMapper extends Mapper<Object, Text, flightMonthlyDelayPair, Text> {
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String strValue = value.toString();
            if (strValue.length() > 0) {
                HashMap<String, String> elements = new HashMap<String, String>();
                Integer start = 0;
                Integer curr = 0;
                // parse the elements in the csv file by ','.
                // need to check edge cases where a comma is in the middle of a single data
                for (int i = 0; i < strValue.length() - 1; i++) {
                    if ((strValue.charAt(i) == ',' && strValue.charAt(i + 1) != ' ')) {
                        for (String k : neededColumnsIndices.keySet()) {
                            if (curr == neededColumnsIndices.get(k)) {
                                elements.put(k, strValue.substring(start, i));
                            }
                        }
                        curr += 1;
                        start = i + 1;
                    }
                }
                //System.out.println(elements);
                // check and filter conditions
                if ((elements.get("Cancelled").equals("0.00") && elements.get("Diverted").equals("0.00"))) {
                //if ((elements.get("Cancelled").equals("\"0\"") && elements.get("Diverted").equals("\"0\""))) {
                    if (elements.get("Year").equals("2008")) {
                        //System.out.println(true);
                        flightMonthlyDelayPair reducerKey = new flightMonthlyDelayPair();
                        String month = elements.get("Month").replaceAll("\"", "");
                        reducerKey.setAirlineID(new Text(elements.get("AirlineID").replaceAll("\"", "")));
                        reducerKey.setMonth(new IntWritable(Integer.valueOf(month)));
                        String delayMin = elements.get("ArrDelayMinutes").replaceAll("\"", "");
                        Text v = new Text(month + "," + delayMin);
                        //System.out.println(v.toString());
                        context.write(reducerKey, v);
                    }
                }
            }
        }
    }

    public static class IntSumReducer extends Reducer<flightMonthlyDelayPair, Text, flightMonthlyDelayPair, StringBuilder> {
        ArrayList<StringBuilder> finalOutput = new ArrayList<StringBuilder>();

        public void reduce(flightMonthlyDelayPair key, Iterable<Text> values, Context context
        ) throws IOException, InterruptedException {
            StringBuilder sortedDelayMinList = new StringBuilder();
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

            for (String k : monthAVG.keySet()) {
                Double avg = 0.00;
                if (monthAVG.get(k).get(0) != 0) {
                    avg = monthAVG.get(k).get(1) / monthAVG.get(k).get(0);
                }
                String format = "(" + k + "," + avg.toString() + ")";
                sortedDelayMinList.append(format);
                sortedDelayMinList.append(",");
            }

            context.write(key, sortedDelayMinList);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "flightComputation");
        job.setJarByClass(flightDataAndComputation.class);

        job.setOutputKeyClass(flightMonthlyDelayPair.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(TokenizerMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setPartitionerClass(flightMonthlyDelayPartitioner.class);
        job.setGroupingComparatorClass(flightMonthlyDelayGroupingComparator.class);
        job.setNumReduceTasks(11);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
