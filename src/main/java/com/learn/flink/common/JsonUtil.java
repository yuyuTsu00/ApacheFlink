package com.learn.flink.common;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learn.flink.model.Transaction;

/**
 * Tiny JSON helper. The ObjectMapper is static (created once, never serialized),
 * so it is safe to call from Flink operators which must be serializable.
 */
public final class JsonUtil {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            // read/write our public fields directly (no getters/setters needed)
            .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private JsonUtil() {
    }

    public static String toJson(Object value) {
        try {
            return MAPPER.writeValueAsString(value);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize to JSON", e);
        }
    }

    public static Transaction toTransaction(String json) {
        try {
            return MAPPER.readValue(json, Transaction.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse Transaction from: " + json, e);
        }
    }
}
