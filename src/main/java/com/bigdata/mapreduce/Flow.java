package com.bigdata.mapreduce;


import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Flow implements WritableComparable<Flow> {

    private int up;
    private int down;

    public Flow() {}

    public Flow(String up, String down) {
        this.up = Integer.parseInt(up);
        this.down = Integer.parseInt(down);
    }

    public int getUp() {
        return this.up;
    }

    public int getDown() {
        return this.down;
    }

    public int getTotal() {
        return this.up + this.down;
    }

    @Override
    public int compareTo(Flow o) {
        int thisValue = getTotal();
        int thatValue = o.getTotal();
        return Integer.compare(thisValue, thatValue);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(up);
        out.writeInt(down);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        up = in.readInt();
        down = in.readInt();
    }
}
