package com.ignite.project.demo.dim;

import lombok.Data;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.lang.IgniteBiPredicate;

import java.util.HashMap;
import java.util.Map;

@Data
public class DimStorePredicate implements IgniteBiPredicate<Integer, BinaryObject> {

    private Map<String,String> cond;

    public DimStorePredicate(Map<String,String> condition) {
        this.cond = condition;
    }

    @Override
    public boolean apply(Integer key, BinaryObject value) {
        return value.<String>field("addr").contains(cond.get("addr"));
    }
}
