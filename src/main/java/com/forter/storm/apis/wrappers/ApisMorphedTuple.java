package com.forter.storm.apis.wrappers;

import backtype.storm.task.GeneralTopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import backtype.storm.utils.Utils;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Extending the base tuple impl in order to supply own equality
 */
public class ApisMorphedTuple extends TupleImpl {
    public static AtomicInteger serialGenerator = new AtomicInteger(0);

    private final int serial;

    public ApisMorphedTuple(Tuple input, List<Object> passThroughParams, GeneralTopologyContext context) {
        this(input, passThroughParams, context, input.getSourceTask());
    }

    public ApisMorphedTuple(Tuple input, List<Object> passThroughParams, GeneralTopologyContext context, Integer taskId) {
        super(context, passThroughParams, taskId, Utils.DEFAULT_STREAM_ID, input.getMessageId());
        this.serial = serialGenerator.getAndAdd(1);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ApisMorphedTuple &&
                ((ApisMorphedTuple) other).serial == this.serial;
    }
}
