package org.example;

import net.minidev.json.JSONUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.*;


public class flightDataAndComputation {
    private static HashMap<Integer, ArrayList<String>> flightInfo = new HashMap<Integer, ArrayList<String>>();
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
        put("FlightDate", headerNames.indexOf("FlightDate"));
        put("Origin", headerNames.indexOf("Origin"));
        put("Dest", headerNames.indexOf("Dest"));
        put("DepTime", headerNames.indexOf("DepTime"));
        put("DepDelayMinutes", headerNames.indexOf("DepDelayMinutes"));
        put("ArrTime", headerNames.indexOf("ArrTime"));
        put("ArrDelayMinutes", headerNames.indexOf("ArrDelayMinutes"));
        put("Cancelled", headerNames.indexOf("Cancelled"));
        put("Diverted", headerNames.indexOf("Diverted"));
    }};

    private static List<String> neededColumns = Arrays.asList("FlightDate", "Origin", "Dest", "DepTime",
            "ArrTime", "ArrDelayMinutes", "Cancelled", "Diverted");
    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
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
                    System.out.println(true);
                    Integer intYM = Integer.valueOf(elements.get("FlightDate").substring(0, 7).replace("-", ""));
                    if (intYM <= 200805 && intYM >= 200706) {
                        if (elements.get("Origin").equals("\"ORD\"") && !elements.get("Dest").equals("\"JFK\"")) {
                            String k = elements.get("FlightDate") + "," + elements.get("Dest");
                            String v = "1" + "," + elements.get("ArrTime").replaceAll("\"|\"", "") +
                                    "," + elements.get("DepTime").replaceAll("\"|\"", "") +
                                    "," + elements.get("ArrDelayMinutes");
                            context.write(new Text(k), new Text(v));
                        } else if (!elements.get("Origin").equals("\"ORD\"") && elements.get("Dest").equals("\"JFK\"")) {
                            String k = elements.get("FlightDate") + "," + elements.get("Origin");
                            String v = "2" + "," + elements.get("ArrTime").replaceAll("\"|\"", "") +
                                    "," + elements.get("DepTime").replaceAll("\"|\"", "") +
                                    "," + elements.get("ArrDelayMinutes");
                            context.write(new Text(k), new Text(v));
                        }
                    }
                }
            }
        }

        // helper function to get key to emit. Set tag, flight date, and destination / origin as key
        public Text getKey(HashMap<String, String> elements, Boolean isF1) {
            String s = "";
            if (isF1) {
                s += elements.get("FlightDate") + "," + elements.get("Dest");
            } else {
                s += elements.get("FlightDate") + "," + elements.get("Origin");
            }
            return new Text(s);
        }

        // helper function to get value to emit. Set arrival time, departure time, delay time as value
        public Text getValue(HashMap<String, String> elements, Boolean isF1) {
            String s = "";
            if (isF1) {
                s += "1" + "," + elements.get("ArrTime").replaceAll("\"|\"", "") +
                        "," + elements.get("DepTime").replaceAll("\"|\"", "") +
                        "," + elements.get("ArrDelayMinutes");
            } else {
                s += "2" + "," + elements.get("ArrTime").replaceAll("\"|\"", "") +
                        "," + elements.get("DepTime").replaceAll("\"|\"", "") +
                        "," + elements.get("ArrDelayMinutes");
            }
            return new Text(s);
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, Text, Text, Text> {
        double count = 0.00;
        double totalDelay = 0.00;

        public void reduce(Text key, Iterable<Text> values, Context context
        ) throws IOException, InterruptedException {
            ArrayList<List<String>> originORDList = new ArrayList<List<String>>();
            ArrayList<List<String>> destJFKList = new ArrayList<List<String>>();
            for (Text value : values) {
                String strValue = value.toString();
                List<String> listValue = Arrays.asList(strValue.split(","));
                if (listValue.get(0).equals("1")) {
                    originORDList.add(listValue);
                } else {
                    destJFKList.add(listValue);
                }
            }

            for (List<String> flightFromORD : originORDList) {
                for (List<String> flightToJFK : destJFKList) {
                    if (Integer.valueOf(flightFromORD.get(1)) < Integer.valueOf(flightToJFK.get(2))) {
                        count += 1;
                        totalDelay += Double.valueOf(flightFromORD.get(3)) + Double.valueOf(flightToJFK.get(3));
                    }
                }
            }

            String v = String.valueOf(count) + ", " + String.valueOf(totalDelay / count) + ", " + totalDelay;
            context.write(new Text("The total count and average delay are:"), new Text(v));

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "flightComputation");
        job.setJarByClass(flightDataAndComputation.class);
        job.setMapperClass(TokenizerMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //job.setPartitionerClass(CustomPartitioner.class);
        job.setNumReduceTasks(11);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
