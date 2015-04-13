package com.forter.storm.apis;

import java.util.List;

/**
 * Configuration for APIs stream construction
 */
public class ApisRemoteCommandTopologyConfig extends ApisTopologyConfig {
    String apiSpout;
    List<String> defaultStreamSpouts;
    String apisStreamName;
    String apisCommandFieldName;
    String apisIdFieldName;
    ApisTransportTopologyConfig trasport;

    protected ApisRemoteCommandTopologyConfig() {}

    /**
     * Create a new API instrumentation bolt wrapper
     * @param bolt the bolt to wrap
     * @param isExcluded is the bolt statically excluded from API stream topology
     * @param defaultStreamSpouts a list of spouts that emit into the default stream. This is assuming they all share
     *                            the same output fields (pretty fair assumption)
     * @param apiSpout the spout that emits into API topology. The wrapper will know to swap its emits with the raw
     *                 input specified in the command
     */

    public String getApiSpout() {
        return apiSpout;
    }

    public List<String> getDefaultStreamSpouts() {
        return defaultStreamSpouts;
    }

    public String getApisStreamName(String originalStream) {
        return apisStreamName + "-" + originalStream;
    }

    public String getApisCommandFieldName() {
        return apisCommandFieldName;
    }

    public String getApisIdFieldName() {
        return apisIdFieldName;
    }

    public boolean isApiStream(String streamId) {
        return streamId.startsWith(apisStreamName + "-");
    }

    public String getBackingStreamName(String sourceStreamId) {
        if (!isApiStream(sourceStreamId)) {
            throw new RuntimeException("Stream " + sourceStreamId + " was not an API backed stream. Failing...");
        }
        return sourceStreamId.substring(apisStreamName.length() + 1);
    }

    @Override
    public ApisTransportTopologyConfig getTransport() {
        return this.trasport;
    }
}
