package com.acme.ride.driver.service.tracing;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import io.opentracing.propagation.TextMap;
import io.vertx.core.json.JsonObject;

public class AmqpTextMapExtractAdapter implements TextMap {

    static final String DASH = "_$dash$_";

    private final Map<String, String> map = new HashMap<>();

    public AmqpTextMapExtractAdapter(JsonObject message) {
        if (message == null) {
            return;
        }
        JsonObject applicationProperties = message.getJsonObject("application_properties");
        if (applicationProperties == null) {
            return;
        }
        Iterator<Map.Entry<String, Object>> iterator = applicationProperties.iterator();
        while (iterator.hasNext()) {
            Map.Entry<String,Object> next = iterator.next();
            if (next.getValue() instanceof String) {
                map.put(decodeDash(next.getKey()), (String) next.getValue());
            }
        }
    }

    @Override
    public Iterator<Map.Entry<String, String>> iterator() {
        return map.entrySet().iterator();
    }

    @Override
    public void put(String key, String value) {
        throw new UnsupportedOperationException(
                "AmqpTextMapExtractAdapter should only be used with Tracer.extract()");
    }

    private String decodeDash(String key) {
        return key.replace(DASH, "-");
    }
}
