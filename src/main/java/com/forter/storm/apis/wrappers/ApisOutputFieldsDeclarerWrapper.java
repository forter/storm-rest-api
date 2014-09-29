package com.forter.storm.apis.wrappers;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.List;

import static com.forter.storm.apis.TopologyApiConstants.STORM_API_COMMAND_FIELD;
import static com.forter.storm.apis.TopologyApiConstants.STORM_API_ID_FIELD;
import static com.forter.storm.apis.TopologyApiConstants.STORM_API_STREAM;

/**
 * Wrapper for the storm output fields declarer. This is used to intercept the bolt's field deceleration and concatenate
 * the default stream fields to the API stream base fields.
*/
public class ApisOutputFieldsDeclarerWrapper implements OutputFieldsDeclarer {
    private final OutputFieldsDeclarer delegate;

    public ApisOutputFieldsDeclarerWrapper(OutputFieldsDeclarer delegate) {
        this.delegate = delegate;
    }

    @Override
    public void declare(Fields fields) {
        declare(false, fields);
    }

    @Override
    public void declare(boolean direct, Fields fields) {
        declareStream(Utils.DEFAULT_STREAM_ID, direct, fields);
    }

    @Override
    public void declareStream(String streamId, Fields fields) {
        declareStream(streamId, false, fields);
    }

    @Override
    public void declareStream(String streamId, boolean direct, Fields fields) {
        if (Utils.DEFAULT_STREAM_ID.equals(streamId)) {
            List<String> apiStreamFields = Lists.newArrayList(STORM_API_ID_FIELD, STORM_API_COMMAND_FIELD);
            Iterables.addAll(apiStreamFields, fields);
            delegate.declareStream(STORM_API_STREAM, new Fields(apiStreamFields));
        }
        delegate.declareStream(streamId, direct, fields);
    }
}
