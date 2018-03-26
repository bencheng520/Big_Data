package com.ben.storm.count.util;

import backtype.storm.Constants;
import backtype.storm.tuple.Tuple;

/**
 * @Author 001289
 * @Date 2018/3/26 23:25
 * @Description ${DESCRIPTION}
 */

public final class TupleHelpers {
    private TupleHelpers() {
    }

    public static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }
}
