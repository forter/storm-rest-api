package com.forter.storm.apis.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.forter.storm.apis.ApisTopologyCommand;
import com.forter.storm.apis.ApisTopologyConfig;

import java.util.List;
import java.util.Map;

/**
 * Bolt that is used solely for un-anchoring tuples for the rest of the topology
 */
public class ApisAwareTupleUnanchoringBolt implements IRichBolt, ApiAware<ApisTopologyCommand> {
    private final String[] outFields;
    private OutputCollector collector;

    public ApisAwareTupleUnanchoringBolt(List<String> outFieldsList) {
        String[] outFields = new String[outFieldsList.size()];
        this.outFields = outFieldsList.toArray(outFields);
    }

    public ApisAwareTupleUnanchoringBolt(String[] outFields) {
        this.outFields = outFields;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        this.collector.emit(input.getValues());
        this.collector.ack(input);
    }

    @Override
    public void execute(Tuple input, ApisTopologyCommand command) {
        this.collector.emit(input.getSourceStreamId(), input, input.getValues());
        this.collector.ack(input);
    }

    @Override
    public void cleanup() { }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(outFields));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() { return null; }

    @Override
    public void setApiConfiguration(ApisTopologyConfig apisConfiguration) {}
}
