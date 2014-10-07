package com.forter.storm.apis;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

/**
 * Singleton object mapper
 */
class ObjectMapperHolder {
    private static final ObjectMapper mapper = new ObjectMapper();
    public static ObjectReader getReader() {
        return mapper.reader();
    }
}