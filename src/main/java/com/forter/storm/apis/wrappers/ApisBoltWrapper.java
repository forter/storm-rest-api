package com.forter.storm.apis.wrappers;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.forter.storm.apis.bolt.ApiSkip;
import com.forter.storm.apis.ApisTopologyCommand;
import com.forter.storm.apis.ApisTopologyConfig;
import com.forter.storm.apis.bolt.ApiAware;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
* A wrapper bolt that will auto-wire the inner bolt for API capabilities
*/
public class ApisBoltWrapper<T extends ApisTopologyCommand> implements IRichBolt {
    private static Logger logger = LoggerFactory.getLogger(ApisBoltWrapper.class);

    private final IRichBolt bolt;
    private final ApisTopologyConfig apisConfiguration;

    private transient String boltIdentification;
    private transient ApisInterceptorOutputCollector interceptorOutputCollector;
    private transient TopologyContext context;

    public ApisBoltWrapper(IRichBolt bolt, ApisTopologyConfig apisConfiguration) {
        this.apisConfiguration = apisConfiguration;
        this.bolt = bolt;
        if (bolt instanceof ApiAware) {
            ((ApiAware)bolt).setApiConfiguration(apisConfiguration);
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.boltIdentification = context.getThisComponentId();

        this.interceptorOutputCollector = new ApisInterceptorOutputCollector(collector, this.apisConfiguration,
                this.bolt instanceof ApiAware, this.boltIdentification);

        this.bolt.prepare(stormConf, context, this.interceptorOutputCollector);
        this.context = context;
    }

    /**
     * This method wraps the execute of all bolts that are capable of being instrumented by API flow. It will create a
     * new tuple that will be passed on down the stream of wrappers to the actual bolt.
     */
    @Override
    public void execute(Tuple input) {
       if (input.getSourceGlobalStreamid().get_streamId().equals(apisConfiguration.getApisStreamName())) {
            //noinspection unchecked
            T apiCommand = (T) input.getValueByField(apisConfiguration.getApisCommandFieldName());

            if (apiCommand.containsBolt(this.boltIdentification)) {
                try {
                    if (this.bolt instanceof ApiAware) {
                        this.interceptorOutputCollector.addEmissionInterception(input, input);
                        //noinspection unchecked
                        ((ApiAware<T>) this.bolt).execute(input, apiCommand);
                        return;
                    }

                    List<Object> passThroughParams = apiCommand.getPredefinedBoltOutput(this.boltIdentification);

                    // in cases we already have a pre-defined output, channel the call directly without going through
                    // default stream
                    if (passThroughParams != null) {
                        List<Object> values = Lists.newArrayList(
                                input.getValueByField(this.apisConfiguration.getApisIdFieldName()), apiCommand);

                        values.addAll(passThroughParams);
                        this.interceptorOutputCollector.emit(this.apisConfiguration.getApisStreamName(), input, values);
                        this.interceptorOutputCollector.ack(input);
                        return;
                    }

                    // call needs to be proxied into the default stream. Get params and create new tuple.
                    passThroughParams = this.getPassThroughFields(input, apiCommand);

                    ApisMorphedTuple morphedTuple;
                    if (this.apisConfiguration.getApiSpout().equals(input.getSourceComponent())) {
                        // If the tuple was received from the API spout, we need to make the morphed tuple believe that the
                        // tuple actually came from the default spout, since the API spout doesn't define proper out fields
                        Integer taskId = getComponentTaskId(this.apisConfiguration.getDefaultStreamSpouts().get(0));
                        morphedTuple = new ApisMorphedTuple(input, passThroughParams, this.context, taskId);
                    } else {
                        morphedTuple = new ApisMorphedTuple(input, passThroughParams, this.context);
                    }

                    this.interceptorOutputCollector.addEmissionInterception(input, morphedTuple);
                    this.bolt.execute(morphedTuple);
                } catch (Exception e) {
                    // TODO: Error handling in API calls
                    logger.warn("Error while executing API call in " + this.boltIdentification + " bolt.", e);
                    this.interceptorOutputCollector.fail(input);
                }
            } else {
                this.interceptorOutputCollector.ack(input);
            }
            return; // return here since we want to skip sub-topologies that were not included in the API filter
        }

        this.bolt.execute(input); // channel the call through regularly if we're not in APIs stream
    }

    @Override
    public void cleanup() {
        this.bolt.cleanup();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        if (!(this.bolt instanceof ApiSkip)) {
            declarer = new ApisOutputFieldsDeclarerWrapper(declarer, this.apisConfiguration);
        }
        this.bolt.declareOutputFields(declarer);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return this.bolt.getComponentConfiguration();
    }

    /**
     * Gets the task ID for the default stream spout. Assumes all default spouts emit the same pattern.
     */
    private Integer getComponentTaskId(String bolt) {
        List<Integer> tasks = this.context.getComponentTasks(bolt);

        Preconditions.checkArgument(!tasks.isEmpty(),
                "No tasks defined for defined defualt stream component %s", bolt);

        return tasks.get(0);
    }

    /**
     * Extract the values that need to be proxied into the default stream from an API stream input tuple
     */
    private List<Object> getPassThroughFields(final Tuple input, ApisTopologyCommand command) {
        // if the tuple is from the API stream, emit the predefined topology input if it exists
        if (this.apisConfiguration.getApiSpout().equals(input.getSourceComponent()) && command.getInput() != null) {
            List<Object> list = command.getInput();
            for (String field : input.getFields()) {
                if (field.startsWith("_")) {
                    list.add(input.getStringByField(field));
                }
            }
            return list;
        }
        // get all non API stream fields to channel into default stream
        Iterable<String> inputFields = Iterables.filter(input.getFields(), new Predicate<String>() {
            @Override
            public boolean apply(String input) {
                return !input.equals(apisConfiguration.getApisIdFieldName()) &&
                        !input.equals(apisConfiguration.getApisCommandFieldName());
            }
        });
        // get their values from current input tuple
        Iterable<Object> iterable = Iterables.transform(inputFields, new Function<String, Object>() {
            @Override
            public Object apply(String field) {
                return input.getValueByField(field);
            }
        });
        return Lists.newArrayList(iterable);
    }
}
