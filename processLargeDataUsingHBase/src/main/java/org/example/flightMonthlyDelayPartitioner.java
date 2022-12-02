package org.example;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class flightMonthlyDelayPartitioner extends Partitioner<flightMonthlyDelayPair, Text> {
    @Override
    public int getPartition(flightMonthlyDelayPair pair, Text v, int numberOfPartitions) {
        // make sure that partitions are non-negative
        return Math.abs(Integer.valueOf(pair.getAirlineID().toString()).hashCode() % numberOfPartitions);
    }
}
