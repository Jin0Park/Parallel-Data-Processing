package org.flightMonthlyDelay;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class flightMonthlyDelayPartitioner extends Partitioner<flightMonthlyDelayPair, flightMonthlyDelayValue> {
    @Override
    public int getPartition(flightMonthlyDelayPair pair, flightMonthlyDelayValue v, int numberOfPartitions) {
        // make sure that partitions are non-negative
        return Math.abs(Integer.valueOf(pair.getAirlineID().toString()).hashCode() % numberOfPartitions);
    }
}
