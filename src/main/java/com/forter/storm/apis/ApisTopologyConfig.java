package com.forter.storm.apis;

import java.io.Serializable;

/**
 * Created by reem on 4/13/15.
 */
public abstract class ApisTopologyConfig implements Serializable {
    public abstract ApisTransportTopologyConfig getTrasport();
}
