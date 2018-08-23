package com.acme.ride.driver.service.tracing;

import java.util.Iterator;
import java.util.Map;

import io.opentracing.propagation.TextMap;
import io.vertx.core.json.JsonObject;

public class AmqpTextMapInjectAdapter implements TextMap {

    static final String DASH = "_$dash$_";

    private JsonObject message;

    public AmqpTextMapInjectAdapter(JsonObject message) {
        this.message = message;
    }

    @Override
    public Iterator<Map.Entry<String, String>> iterator() {
        throw new UnsupportedOperationException("iterator should never be used with Tracer.injectSpan()");
    }

    @Override
    public void put(String key, String value) {
        if (message == null) {
            return;
        }
        JsonObject applicationProperties = message.getJsonObject("application_properties");
        if (applicationProperties == null) {
            applicationProperties = new JsonObject();
            message.put("application_properties", applicationProperties);
        }
        applicationProperties.put(encodeDash(key), value);
    }

    private String encodeDash(String key) {
        if (key == null || key.isEmpty()) {
            return key;
        }

        return key.replace("-", DASH);
    }
}
