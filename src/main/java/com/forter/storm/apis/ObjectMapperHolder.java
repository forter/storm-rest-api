package com.forter.storm.apis;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;

/**
 * Singleton object mapper
 */
public class ObjectMapperHolder {
    private static final ObjectMapper mapper = new ObjectMapper();
    public static ObjectReader getReader() {
        return mapper.reader();
    }
    public static ObjectWriter getWriter() {
        return mapper.writer();
    }
}