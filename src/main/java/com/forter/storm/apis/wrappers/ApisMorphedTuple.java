package com.forter.storm.apis.wrappers;

import org.apache.storm.task.GeneralTopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.utils.Utils;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Extending the base tuple impl in order to supply own equality
 */
public class ApisMorphedTuple extends TupleImpl {
    public ApisMorphedTuple(Tuple input, List<Object> passThroughParams, String stream, GeneralTopologyContext context) {
        this(input, passThroughParams, stream, context, input.getSourceTask());
    }

    public ApisMorphedTuple(Tuple input, List<Object> passThroughParams, String stream, GeneralTopologyContext context, Integer taskId) {
        super(context, passThroughParams, taskId, stream, input.getMessageId());
    }
}
