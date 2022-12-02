package org.flightMonthlyDelay;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class flightMonthlyDelayGroupingComparator extends WritableComparator {
    public flightMonthlyDelayGroupingComparator() {
        super(flightMonthlyDelayPair.class, true);
        }
        @Override
        /**
          * This comparator controls which keys are grouped
          * together into a single call to the reduce() method
          */
        public int compare(WritableComparable wc1, WritableComparable wc2) {
            flightMonthlyDelayPair p1 = (flightMonthlyDelayPair) wc1;
            flightMonthlyDelayPair p2 = (flightMonthlyDelayPair) wc2;

            int comparison = p1.getAirlineID().compareTo(p2.getAirlineID());
            return comparison;

//            if (comparison == 0) {
//                if (p1.getMonth() == p2.getMonth()) {
//                    return 0;
//                } else if (p1.getMonth().get() < p2.getMonth().get()) {
//                    return -1;
//                } else {
//                    return 1;
//                }
//            } else {
//                return comparison;
//            }
        }
}
