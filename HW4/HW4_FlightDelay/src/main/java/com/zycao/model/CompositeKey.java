package com.zycao.model;

import lombok.Data;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
public class CompositeKey implements WritableComparable<CompositeKey> {
    private String carrier;
    private int month;

    public CompositeKey() {}

    public CompositeKey(String carrier, int month) {
        this.carrier = carrier;
        this.month = month;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(carrier);
        out.writeInt(month);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.carrier = in.readUTF();
        this.month = in.readInt();
    }

    @Override
    public int compareTo(CompositeKey o) {
        int result = this.carrier.compareTo(o.carrier);
        if (result != 0) {
            return result;
        }
        return Integer.compare(this.month, o.month);
    }
}
