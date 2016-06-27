package com.forter.storm.apis.wrappers;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Grouping;
import org.apache.storm.generated.NullStruct;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.grouping.PartialKeyGrouping;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import com.forter.storm.apis.ApisRemoteCommandTopologyConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
* Created by reem on 9/24/14.
*/
public class APIsBoltDeclarerWrapper implements BoltDeclarer {
    private final BoltDeclarer delegate;
    private final ApisRemoteCommandTopologyConfig apisConfiguration;

    public APIsBoltDeclarerWrapper(BoltDeclarer delegate, ApisRemoteCommandTopologyConfig config) {
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
    public BoltDeclarer partialKeyGrouping(String componentId, Fields fields) {
        return this.customGrouping(componentId, new PartialKeyGrouping(fields));
    }

    @Override
    public BoltDeclarer partialKeyGrouping(String componentId, String streamId, Fields fields) {
        return this.customGrouping(componentId, streamId, new PartialKeyGrouping(fields));
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
        return this.grouping(id.get_componentId(), id.get_streamId(), grouping);
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

    @Override
    public BoltDeclarer setMemoryLoad(Number number) {
        return this.delegate.setMemoryLoad(number);
    }

    @Override
    public BoltDeclarer setMemoryLoad(Number number, Number number1) {
        return this.delegate.setMemoryLoad(number, number1);
    }

    @Override
    public BoltDeclarer setCPULoad(Number number) {
        return this.delegate.setCPULoad(number);
    }

    protected BoltDeclarer grouping(String componentId, String streamId, Grouping grouping) {
        delegate.grouping(new GlobalStreamId(componentId, streamId), grouping);

        final String STORM_API_SPOUT = apisConfiguration.getApiSpout();
        final List<String> DEFAULT_STREAM_SPOUTS = apisConfiguration.getDefaultStreamSpouts();

        if (!apisConfiguration.isApiStream(streamId) && !apisConfiguration.getDefaultStreamSpouts().contains(componentId)) {
             this.grouping(new GlobalStreamId(componentId, apisConfiguration.getApisStreamName(streamId)), grouping);
        } else if (DEFAULT_STREAM_SPOUTS.contains(componentId)) {
            if (apisConfiguration.isApiStream(streamId)) {
                this.grouping(new GlobalStreamId(STORM_API_SPOUT, streamId), grouping);
            } else {
                this.grouping(new GlobalStreamId(STORM_API_SPOUT, apisConfiguration.getApisStreamName(streamId)), grouping);
            }
        }

        return this;
    }
}
