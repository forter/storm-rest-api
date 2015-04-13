package com.forter.storm.apis.impl.redis;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.forter.storm.apis.ApiSinkBolt;
import com.forter.storm.apis.ObjectMapperHolder;
import com.google.common.base.Throwables;
import redis.clients.jedis.Jedis;

import java.util.Map;

/**
 * Created by reem on 10/7/14.
 */
public class RedisApiSinkBolt extends ApiSinkBolt {
    private transient ObjectWriter writer;
    private transient Jedis jedis;
    private RedisApisConfiguration configuration;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        this.configuration = (RedisApisConfiguration) apisConfiguration.getTransport();
        this.writer = ObjectMapperHolder.getWriter();
        this.jedis = new Jedis(configuration.getApisRedisHost(), configuration.getApisRedisPort());
    }

    @Override
    protected void registerApiResult(final String id, final ObjectNode response) {
        try {
            jedis.publish(configuration.getApisRedisResponseChannel() + "." + id, writer.writeValueAsString(response));
        } catch (JsonProcessingException e) {
            throw Throwables.propagate(e);
        }
    }
}
