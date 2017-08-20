package com.newnius.code4hadoop.kmeans;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by newnius on 8/20/17.
 *
 */
public class Cluster implements Writable {
    private int id;
    private int num;
    private double x;
    private double y;

    public Cluster() {
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    public double getX() {
        return x;
    }

    public void setX(double x) {
        this.x = x;
    }

    public double getY() {
        return y;
    }

    public void setY(double y) {
        this.y = y;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        StringBuilder sb = new StringBuilder("");
        sb.append(id).append("\t");
        sb.append(num).append("\t");
        sb.append(x).append(",").append(y);
        try {
            dataOutput.write(sb.toString().getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        String line = dataInput.readLine();
        fromString(line);
    }

    public void fromString(String line){
        String[] tuple = line.split("\t");
        id = Integer.parseInt(tuple[0]);
        num = Integer.parseInt(tuple[1]);
        String[] points = tuple[2].split(",");
        x = Double.parseDouble(points[0]);
        y = Double.parseDouble(points[1]);
    }

    @Override
    public String toString() {
        return "" + id + "\t" +
                num + "\t" +
                x + "," + y;
    }
}
