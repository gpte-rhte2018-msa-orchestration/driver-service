package com.acme.ride.driver.service.tracing;

import io.opentracing.References;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import io.opentracing.tag.Tags;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

public class TracingUtils {

    public static final String OPERATION_NAME_SEND = "amqp-send";
    public static final String OPERATION_NAME_RECEIVE = "amqp-receive";
    public static final String COMPONENT_NAME = "vertx-amqp";

    public static Scope buildFollowingSpan(JsonObject message, Tracer tracer) {

        SpanContext context = extract(message, tracer);

        if (context != null) {
            Tracer.SpanBuilder spanBuilder = tracer.buildSpan(OPERATION_NAME_RECEIVE)
                    .ignoreActiveSpan()
                    .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CONSUMER);

            spanBuilder.addReference(References.FOLLOWS_FROM, context);
            Scope scope = spanBuilder.startActive(true);
            Tags.COMPONENT.set(scope.span(), COMPONENT_NAME);
            return scope;
        }

        return null;
    }

    public static Span buildAndInjectSpan(JsonObject amqpMessage, Tracer tracer, Message<? extends Object> msg) {
        Tracer.SpanBuilder spanBuilder = tracer.buildSpan(OPERATION_NAME_SEND)
                .ignoreActiveSpan()
                .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_PRODUCER);

        SpanContext parent = extract(msg, tracer);
        if (parent != null) {
            spanBuilder.asChildOf(parent);
        }
        Span span = spanBuilder.start();
        Tags.COMPONENT.set(span, TracingUtils.COMPONENT_NAME);
        injectSpan(span, amqpMessage, tracer);

        return span;
    }

    public static SpanContext extract(JsonObject message, Tracer tracer) {
        SpanContext spanContext = tracer.extract(Format.Builtin.TEXT_MAP, new AmqpTextMapExtractAdapter(message));
        if (spanContext != null) {
            return spanContext;
        }

        Span span = tracer.activeSpan();
        if (span != null) {
            return span.context();
        }
        return null;
    }

    public static void injectSpan(Span span, JsonObject message, Tracer tracer) {
        tracer.inject(span.context(), Format.Builtin.TEXT_MAP, new AmqpTextMapInjectAdapter(message));
    }

    public static DeliveryOptions injectSpan(DeliveryOptions options, Tracer tracer) {
        Span span = tracer.activeSpan();
        if (span != null) {
            options.addHeader("opentracing.span", span.context().toString());
        }
        return options;
    }

    public static SpanContext extract(Message<? extends Object> msg, Tracer tracer) {
        String spanContextAsString = msg.headers().get("opentracing.span");
        if (spanContextAsString != null) {
            return io.jaegertracing.SpanContext.contextFromString(spanContextAsString);
        }
        Span span = tracer.activeSpan();
        if (span != null) {
            return span.context();
        }
        return null;
    }

}
