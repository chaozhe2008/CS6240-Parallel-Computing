package com.zycao.secondarySort;

import com.zycao.model.CompositeKey;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparator extends WritableComparator {
    protected GroupingComparator() {
        super(CompositeKey.class, true);
    }

    @Override
    /**
     * /**
     * This comparator controls which keys are grouped
     * together into a single call to the reduce() method
     */
    public int compare(WritableComparable w1, WritableComparable w2) {
        CompositeKey key1 = (CompositeKey) w1;
        CompositeKey key2 = (CompositeKey) w2;
        return key1.getCarrier().compareTo(key2.getCarrier());
    }
}
