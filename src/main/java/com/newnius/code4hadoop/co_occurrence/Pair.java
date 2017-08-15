package com.newnius.code4hadoop.co_occurrence;

import com.sun.corba.se.spi.ior.Writeable;
import org.apache.hadoop.io.WritableComparable;
import org.omg.CORBA_2_3.portable.OutputStream;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by newnius on 8/15/17.
 *
 */
public class Pair implements WritableComparable<Pair> {
    private String wordA;
    private String wordB;

    public Pair() {
    }

    public Pair(String wordA, String wordB) {
        this.wordA = wordA;
        this.wordB = wordB;
    }

    public String getWordA() {
        return wordA;
    }

    public void setWordA(String wordA) {
        this.wordA = wordA;
    }

    public String getWordB() {
        return wordB;
    }

    public void setWordB(String wordB) {
        this.wordB = wordB;
    }

    @Override
    public int hashCode() {
        return wordA.hashCode() + wordB.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof Pair && obj.hashCode() == hashCode();
    }

    @Override
    public int compareTo(Pair o) {
        return o.hashCode() - hashCode();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.write((wordA+","+wordB).getBytes());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        String[] words = dataInput.readLine().split(",");
        wordA = words[0];
        wordB = words[1];
    }

    @Override
    public String toString() {
        return wordA+","+wordB;
    }
}
