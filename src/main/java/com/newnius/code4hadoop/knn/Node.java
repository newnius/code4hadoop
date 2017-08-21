package com.newnius.code4hadoop.knn;

/**
 * Created by newnius on 8/20/17.
 */
public class Node {
    private int id;
    private double x;
    private double y;
    private int type;

    public Node() {
    }

    public Node(int id, double x, double y) {
        this.id = id;
        this.x = x;
        this.y = y;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
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

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public void fromString(String line){
        String[] t = line.split("\t");
        id = Integer.parseInt(t[0]);
        x = Integer.parseInt(t[1].split(",")[0]);
        y = Integer.parseInt(t[1].split(",")[1]);
        if(t.length == 3){
            type = Integer.parseInt(t[2]);
        }
    }
}
