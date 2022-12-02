package org.flightMonthlyDelay;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.example.flightDataAndComputation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;


public class flightMonthlyDelayAvg {

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

    public static class TokenizerMapper extends Mapper<Object, Text, flightMonthlyDelayPair, flightMonthlyDelayValue> {
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
                System.out.println(elements);
                // check and filter conditions
                if ((elements.get("Cancelled").equals("0.00") && elements.get("Diverted").equals("0.00"))) {
                    if (elements.get("Year").equals("2008")) {
                        flightMonthlyDelayPair reducerKey = new flightMonthlyDelayPair();
                        String month = elements.get("Month").replaceAll("\"", "");
                        reducerKey.setAirlineID(new Text(elements.get("AirlineID").replaceAll("\"", "")));
                        reducerKey.setMonth(new IntWritable(Integer.valueOf(month)));
                        flightMonthlyDelayValue values = new flightMonthlyDelayValue();
                        String delayMin = elements.get("ArrDelayMinutes").replaceAll("\"", "");
                        values.setMonth(new IntWritable((Integer.valueOf(month))));
                        values.setDelayMin(new IntWritable((Double.valueOf(delayMin)).intValue()));
                        //Text v = new Text(month + "," + delayMin);
                        context.write(reducerKey, values);
                    }
                }
            }
        }
    }

    public static class IntSumReducer extends Reducer<flightMonthlyDelayPair, flightMonthlyDelayValue, flightMonthlyDelayPair, StringBuilder> {

        public void reduce(flightMonthlyDelayPair key, Iterable<flightMonthlyDelayValue> values, Context context
        ) throws IOException, InterruptedException {
            StringBuilder sortedDelayMinList = new StringBuilder();
            Integer currMonth = 1;
            Integer currCount = 0;
            Integer currSum = 0;
            for (flightMonthlyDelayValue v : values) {
                if (v.getMonth().get() == currMonth) {
                    currCount += 1;
                    currSum += v.getDelayMin().get();
                } else {
                    Integer avg = 0;
                    if (currCount > 0) {
                        avg = (int) Math.ceil(currSum / currCount);
                    }
                    sortedDelayMinList.append("(" + currMonth.toString() + "," + avg.toString() + ")");
                    sortedDelayMinList.append(",");
                    currSum = 0;
                    currCount = 0;
                    currMonth += 1;
                }

            }
            context.write(key, sortedDelayMinList);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "flightComputation");
        job.setJarByClass(flightDataAndComputation.class);

        job.setOutputKeyClass(flightMonthlyDelayPair.class);
        job.setOutputValueClass(flightMonthlyDelayValue.class);
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
