package com.forter.storm.apis.wrappers;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.generated.NullStruct;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import com.forter.storm.apis.ApisTopologyConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
* Created by reem on 9/24/14.
*/
public class APIsBoltDeclarerWrapper implements BoltDeclarer {
    private final BoltDeclarer delegate;
    private final ApisTopologyConfig apisConfiguration;

    public APIsBoltDeclarerWrapper(BoltDeclarer delegate, ApisTopologyConfig config) {
        this.delegate = delegate;
        this.apisConfiguration = config;
    }

    @Override
    public BoltDeclarer fieldsGrouping(String componentId, Fields fields) {
        return fieldsGrouping(componentId, Utils.DEFAULT_STREAM_ID, fields);
    }

    @Override
    public BoltDeclarer fieldsGrouping(String componentId, String streamId, Fields fields) {
        return grouping(componentId, streamId, Grouping.fields(fields.toList()));
    }

    @Override
    public BoltDeclarer globalGrouping(String componentId) {
        return globalGrouping(componentId, Utils.DEFAULT_STREAM_ID);
    }

    @Override
    public BoltDeclarer globalGrouping(String componentId, String streamId) {
        return grouping(componentId, streamId, Grouping.fields(new ArrayList<String>()));
    }

    @Override
    public BoltDeclarer shuffleGrouping(String componentId) {
        return shuffleGrouping(componentId, Utils.DEFAULT_STREAM_ID);
    }

    @Override
    public BoltDeclarer shuffleGrouping(String componentId, String streamId) {
        return grouping(componentId, streamId, Grouping.shuffle(new NullStruct()));
    }

    @Override
    public BoltDeclarer localOrShuffleGrouping(String componentId) {
        return localOrShuffleGrouping(componentId, Utils.DEFAULT_STREAM_ID);
    }

    @Override
    public BoltDeclarer localOrShuffleGrouping(String componentId, String streamId) {
        return grouping(componentId, streamId, Grouping.local_or_shuffle(new NullStruct()));
    }

    @Override
    public BoltDeclarer noneGrouping(String componentId) {
        return noneGrouping(componentId, Utils.DEFAULT_STREAM_ID);
    }

    @Override
    public BoltDeclarer noneGrouping(String componentId, String streamId) {
        return grouping(componentId, streamId, Grouping.none(new NullStruct()));
    }

    @Override
    public BoltDeclarer allGrouping(String componentId) {
        return allGrouping(componentId, Utils.DEFAULT_STREAM_ID);
    }

    @Override
    public BoltDeclarer allGrouping(String componentId, String streamId) {
        return grouping(componentId, streamId, Grouping.all(new NullStruct()));
    }

    @Override
    public BoltDeclarer directGrouping(String componentId) {
        return directGrouping(componentId, Utils.DEFAULT_STREAM_ID);
    }

    @Override
    public BoltDeclarer directGrouping(String componentId, String streamId) {
        return grouping(componentId, streamId, Grouping.direct(new NullStruct()));
    }

    @Override
    public BoltDeclarer customGrouping(String componentId, CustomStreamGrouping grouping) {
        return customGrouping(componentId, Utils.DEFAULT_STREAM_ID, grouping);
    }

    @Override
    public BoltDeclarer customGrouping(String componentId, String streamId, CustomStreamGrouping grouping) {
        return grouping(componentId, streamId, Grouping.custom_serialized(Utils.serialize(grouping)));
    }

    @Override
    public BoltDeclarer grouping(GlobalStreamId id, Grouping grouping) {
        return delegate.grouping(id, grouping);
    }

    @Override
    public BoltDeclarer addConfigurations(Map conf) {
        return delegate.addConfigurations(conf);
    }

    @Override
    public BoltDeclarer addConfiguration(String config, Object value) {
        return delegate.addConfiguration(config, value);
    }

    @Override
    public BoltDeclarer setDebug(boolean debug) {
        return delegate.setDebug(debug);
    }

    @Override
    public BoltDeclarer setMaxTaskParallelism(Number val) {
        return delegate.setMaxTaskParallelism(val);
    }

    @Override
    public BoltDeclarer setMaxSpoutPending(Number val) {
        return delegate.setMaxSpoutPending(val);
    }

    @Override
    public BoltDeclarer setNumTasks(Number val) {
        return delegate.setNumTasks(val);
    }

    protected BoltDeclarer grouping(String componentId, String streamId, Grouping grouping) {
        this.grouping(new GlobalStreamId(componentId, streamId), grouping);

        final String STORM_API_STREAM = apisConfiguration.getApisStreamName();
        final String STORM_API_SPOUT = apisConfiguration.getApiSpout();
        final List<String> DEFAULT_STREAM_SPOUTS = apisConfiguration.getDefaultStreamSpouts();

        if (Utils.DEFAULT_STREAM_ID.equals(streamId) && !apisConfiguration.getDefaultStreamSpouts().contains(componentId)) {
            this.grouping(new GlobalStreamId(componentId, STORM_API_STREAM), grouping);
        } else if (STORM_API_STREAM.equals(streamId) && DEFAULT_STREAM_SPOUTS.contains(componentId)) {
            this.grouping(new GlobalStreamId(STORM_API_SPOUT, STORM_API_STREAM), grouping);
        } else if (DEFAULT_STREAM_SPOUTS.contains(componentId)) {
            this.grouping(new GlobalStreamId(STORM_API_SPOUT, STORM_API_STREAM), grouping);
        }

        return this;
    }
}
