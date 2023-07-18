package com.opstty;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TreeInfoWritable implements Writable {
    private int district;
    private int year;

    public TreeInfoWritable() {
    }

    public TreeInfoWritable(int district, int year) {
        this.district = district;
        this.year = year;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(district);
        out.writeInt(year);
    }

    public void readFields(DataInput in) throws IOException {
        district = in.readInt();
        year = in.readInt();
    }

    public int getDistrict() {
        return district;
    }

    public int getYear() {
        return year;
    }
}
