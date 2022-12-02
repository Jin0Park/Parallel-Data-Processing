package org.example;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class flightMonthlyDelayValue implements Writable, WritableComparable<flightMonthlyDelayValue> {
    private IntWritable month = new IntWritable();  // secondary key
    private IntWritable delayMin = new IntWritable();


    public IntWritable getMonth() {
        return month;
    }
    public IntWritable getDelayMin() {
        return delayMin;
    }

    public void setMonth(IntWritable month) {
        this.month = month;
    }
    public void setDelayMin(IntWritable delayMin) {
        this.delayMin = delayMin;
    }

    @Override
    public int compareTo(flightMonthlyDelayValue other) {
        return this.delayMin.compareTo(other.delayMin);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.month.get());
        dataOutput.writeInt(this.delayMin.get());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.month = new IntWritable(dataInput.readInt());
        this.delayMin = new IntWritable(dataInput.readInt());
    }
}
