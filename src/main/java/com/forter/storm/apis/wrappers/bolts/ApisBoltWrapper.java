package com.forter.storm.apis.wrappers.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.forter.storm.apis.ApiAware;
import com.forter.storm.apis.ApiInstrumentedJoinBolt;
import com.forter.storm.apis.ApiTopologyErrorHandler;
import com.forter.storm.apis.ApisTopologyCommand;
import com.forter.storm.apis.wrappers.ApisInterceptorOutputCollector;
import com.forter.storm.apis.wrappers.ApisMorphedTuple;
import com.forter.storm.apis.wrappers.ApisOutputFieldsDeclarerWrapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.forter.storm.apis.TopologyApiConstants.STORM_API_COMMAND_FIELD;
import static com.forter.storm.apis.TopologyApiConstants.STORM_API_ID_FIELD;
import static com.forter.storm.apis.TopologyApiConstants.STORM_API_STREAM;

/**
* A wrapper bolt that will auto-wire the inner bolt for API capabilities
*/
public class ApisBoltWrapper implements IRichBolt {
    private final IRichBolt bolt;
    private final boolean isExcluded;
    private final List<String> defaultStreamSpouts;
    private final String apiSpout;

    private transient String boltIdentification;
    private transient ApisInterceptorOutputCollector interceptorOutputCollector;
    private transient TopologyContext context;
    private transient ApiTopologyErrorHandler errorHandler;

    /**
     * Create a new API instrumentation bolt wrapper
     * @param bolt the bolt to wrap
     * @param isExcluded is the bolt statically excluded from API stream topology
     * @param defaultStreamSpouts a list of spouts that emit into the default stream. This is assuming they all share
     *                            the same output fields (pretty fair assumption)
     * @param apiSpout the spout that emits into API topology. The wrapper will know to swap its emits with the raw
     *                 input specified in the command
     */
    public ApisBoltWrapper(IRichBolt bolt, boolean isExcluded, List<String> defaultStreamSpouts, String apiSpout) {
        Preconditions.checkArgument(!defaultStreamSpouts.isEmpty());

        this.bolt = bolt;
        this.isExcluded = isExcluded;
        this.defaultStreamSpouts = defaultStreamSpouts;
        this.apiSpout = apiSpout;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.boltIdentification = context.getThisComponentId();
        this.interceptorOutputCollector = new ApisInterceptorOutputCollector(collector);
        this.bolt.prepare(stormConf, context, this.interceptorOutputCollector);
        this.context = context;
    }

    /**
     * This method wraps the execute of all bolts that are capable of being instrumented by API flow. It will create a
     * new tuple that will be passed on down the stream of wrappers to the actual bolt.
     */
    @Override
    public void execute(Tuple input) {
        if (input.getSourceGlobalStreamid().get_streamId().equals(STORM_API_STREAM)) {
            if (isExcluded) {
                this.interceptorOutputCollector.ack(input);
                return;
            }

            ApisTopologyCommand apiCommand = (ApisTopologyCommand) input.getValueByField(STORM_API_COMMAND_FIELD);

            if (this.bolt instanceof ApiAware) {
                this.bolt.execute(input);
                return;
            }

            if (apiCommand.containsBolt(this.boltIdentification)) {
                try {
                    List<Object> passThroughParams = apiCommand.getPredefinedBoltOutput(this.boltIdentification);

                    // in cases we already have a pre-defined output, channel the call directly without going through
                    // default stream
                    if (passThroughParams != null) {
                        List<Object> values = Lists.newArrayList(input.getValueByField(STORM_API_ID_FIELD), apiCommand);
                        values.addAll(passThroughParams);
                        this.interceptorOutputCollector.emit(STORM_API_STREAM, input, values);
                        this.interceptorOutputCollector.ack(input);
                        return;
                    }

                    // call needs to be proxied into the default stream. Get params and create new tuple.
                    passThroughParams = this.getPassThroughFields(input, apiCommand);

                    ApisMorphedTuple morphedTuple;
                    if (apiSpout.equals(input.getSourceComponent())) {
                        // If the tuple was received from the API spout, we need to make the morphed tuple believe that the
                        // tuple actually came from the default spout, since the API spout doesn't define proper out fields
                        Integer taskId = getComponentTaskId(defaultStreamSpouts.get(0));
                        morphedTuple = new ApisMorphedTuple(input, passThroughParams, this.context, taskId);
                    } else {
                        morphedTuple = new ApisMorphedTuple(input, passThroughParams, this.context);
                    }

                    this.interceptorOutputCollector.addEmissionInterception(input, morphedTuple);

                    Set<String> fields = apiCommand.getJoinWaitFor(this.boltIdentification);
                    if (this.bolt instanceof ApiInstrumentedJoinBolt && fields != null) {
                        ((ApiInstrumentedJoinBolt) this.bolt).executeExpected(morphedTuple, fields);
                    } else {
                        this.bolt.execute(morphedTuple);
                    }
                } catch (Exception e) {
                    if (errorHandler != null) {
                        errorHandler.reportApiError("An error has ocurred while executing API command in " + this.boltIdentification + " bolt.", e, input);
                    }
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
        if (!(this.bolt instanceof ApiAware)) {
            declarer = new ApisOutputFieldsDeclarerWrapper(declarer);
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
        if (apiSpout.equals(input.getSourceComponent()) && command.getInput() != null) {
            return command.getInput();
        }
        // get all non API stream fields to channel into default stream
        Iterable<String> inputFields = Iterables.filter(input.getFields(), new Predicate<String>() {
            @Override
            public boolean apply(String input) {
                return !input.equals(STORM_API_ID_FIELD) && !input.equals(STORM_API_COMMAND_FIELD);
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

    public void setErrorHandler(ApiTopologyErrorHandler errorHandler) {
        this.errorHandler = errorHandler;
    }
}
