package org.example;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class flightMonthlyDelayPair implements Writable, WritableComparable<flightMonthlyDelayPair> {
    private Text airlineID = new Text();  // natural key
    private IntWritable month = new IntWritable();  // secondary key

    public Text getAirlineID() {
        return airlineID;
    }

    public IntWritable getMonth() {
        return month;
    }

    public void setAirlineID(Text airlineID) {
        this.airlineID = airlineID;
    }

    public void setMonth(IntWritable month) {
        this.month = month;
    }

    @Override
    public int compareTo(flightMonthlyDelayPair other) {
        Integer compareValue = this.airlineID.compareTo(other.airlineID);
        if (compareValue == 0) {
            compareValue = this.month.compareTo(other.month);
        }
        return compareValue;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(String.valueOf(this.airlineID));
        dataOutput.writeInt(this.month.get());
        //dataOutput.writeLong(this.arrDelayMin.get());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.airlineID = new Text(dataInput.readUTF());
        this.month = new IntWritable(dataInput.readInt());
    }

    @Override
    public String toString() {
        return String.valueOf(airlineID);
    }
}
