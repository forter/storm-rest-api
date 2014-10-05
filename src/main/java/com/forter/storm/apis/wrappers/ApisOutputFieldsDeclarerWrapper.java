package com.forter.storm.apis.wrappers;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import com.forter.storm.apis.ApisTopologyConfig;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Wrapper for the storm output fields declarer. This is used to intercept the bolt's field deceleration and concatenate
 * the default stream fields to the API stream base fields.
*/
public class ApisOutputFieldsDeclarerWrapper implements OutputFieldsDeclarer {
    private final OutputFieldsDeclarer delegate;

    private final String apisIdFieldName;
    private final String apisCommandFieldName;
    private final String apisStreamName;

    public ApisOutputFieldsDeclarerWrapper(OutputFieldsDeclarer delegate, ApisTopologyConfig apisConfiguration) {
        this.delegate = delegate;
        this.apisIdFieldName = apisConfiguration.getApisIdFieldName();
        this.apisStreamName = apisConfiguration.getApisStreamName();
        this.apisCommandFieldName = apisConfiguration.getApisCommandFieldName();
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
            List<String> apiStreamFields = Lists.newArrayList(apisIdFieldName, apisCommandFieldName);
            Iterables.addAll(apiStreamFields, fields);
            delegate.declareStream(apisStreamName, new Fields(apiStreamFields));
        }
        delegate.declareStream(streamId, direct, fields);
    }
}
