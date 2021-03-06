package com.forter.storm.apis.wrappers;

import backtype.storm.task.GeneralTopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;

import java.util.List;

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
