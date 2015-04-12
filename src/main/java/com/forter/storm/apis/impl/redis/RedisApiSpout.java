package com.forter.storm.apis.impl.redis;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.forter.storm.apis.ApiRequestSpout;
import com.forter.storm.apis.ApisTopologyCommand;
import com.forter.storm.apis.ApisTopologyConfig;
import com.forter.storm.apis.ObjectMapperHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.Map;

/**
 * Created by reem on 10/7/14.
 */
public abstract class RedisApiSpout extends ApiRequestSpout {
    private static Logger logger = LoggerFactory.getLogger(RedisApiSpout.class);

    private final RedisApisConfiguration config;

    private transient Jedis jedis;
    private transient ObjectWriter writer;

    public RedisApiSpout(ApisTopologyConfig config) {
        super(config);
        this.config = (RedisApisConfiguration)config;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        super.open(conf, context, collector);
        jedis = new Jedis(config.getApisRedisHost(), config.getApisRedisPort());
        writer = ObjectMapperHolder.getWriter();
    }

    @Override
    protected void reportError(String id, ObjectNode error) {
        try {
            this.jedis.lpush(config.getApisRedisResponseChannel(), writer.writeValueAsString(error));
        } catch (JsonProcessingException e) {
            logger.warn("Error while reporting error", e);
        }
    }

    @Override
    public void close() {
        super.close();
        this.jedis.disconnect();
    }

    @Override
    protected String getApiCommandJson() {
        return this.jedis.rpop(config.getApisRedisRequestQueue());
    }
}
