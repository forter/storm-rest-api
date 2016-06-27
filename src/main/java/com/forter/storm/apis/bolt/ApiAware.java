package com.forter.storm.apis.bolt;

import org.apache.storm.topology.IComponent;
import org.apache.storm.tuple.Tuple;
import com.forter.storm.apis.ApisRemoteCommandTopologyConfig;
import com.forter.storm.apis.ApisTopologyCommand;

/**
 * Implemented by bolts who want to be aware of the API instrumentation command
 */
public interface ApiAware<T extends ApisTopologyCommand> extends IComponent {
    void execute(Tuple input, T command);
    void setApiConfiguration(ApisRemoteCommandTopologyConfig apisConfiguration);
}