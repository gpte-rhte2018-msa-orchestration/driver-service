package com.acme.ride.driver.service.tracing;

import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.contrib.jms.common.JmsTextMapInjectAdapter;
import io.opentracing.propagation.Format;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;

public class TracingUtils {

    public static void injectParentSpan(Message<? extends Object> vertxMessage, javax.jms.Message jmsMessage, Tracer tracer) {
        SpanContext spanContext = extract(vertxMessage, tracer);
        if (spanContext != null) {
            tracer.inject(spanContext, Format.Builtin.TEXT_MAP, new JmsTextMapInjectAdapter(jmsMessage));
        }
    }

    public static DeliveryOptions injectSpan(Span activeSpan, DeliveryOptions options, Tracer tracer) {
        if (activeSpan != null) {
            options.addHeader("opentracing.span", activeSpan.context().toString());
        }
        return options;
    }

    public static SpanContext extract(Message<? extends Object> msg, Tracer tracer) {
        String spanContextAsString = msg.headers().get("opentracing.span");
        if (spanContextAsString != null) {
            return io.jaegertracing.SpanContext.contextFromString(spanContextAsString);
        }
        return null;
    }

}
