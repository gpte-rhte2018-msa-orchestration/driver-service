package com.acme.ride.driver.service;

import java.io.UnsupportedEncodingException;

import com.acme.ride.driver.service.tracing.TracingUtils;
import io.opentracing.Scope;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;
import io.vertx.amqpbridge.AmqpBridge;
import io.vertx.amqpbridge.AmqpBridgeOptions;
import io.vertx.amqpbridge.AmqpConstants;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.JksOptions;

public class MessageConsumerVerticle extends AbstractVerticle {

    private final static Logger log = LoggerFactory.getLogger("MessageConsumer");

    private AmqpBridge bridge;

    private Tracer tracer;

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        AmqpBridgeOptions bridgeOptions = new AmqpBridgeOptions();
        bridgeOptions.setSsl(config().getBoolean("amqp.ssl"));
        bridgeOptions.setTrustAll(config().getBoolean("amqp.ssl.trustall"));
        bridgeOptions.setHostnameVerificationAlgorithm(!config().getBoolean("amqp.ssl.verifyhost") ? "" : "HTTPS");
        bridgeOptions.setReplyHandlingSupport(config().getBoolean("amqp.replyhandling"));
        if (!bridgeOptions.isTrustAll()) {
            JksOptions jksOptions = new JksOptions()
                    .setPath(config().getString("amqp.truststore.path"))
                    .setPassword(config().getString("amqp.truststore.password"));
            bridgeOptions.setTrustStoreOptions(jksOptions);
        }
        bridge = AmqpBridge.create(vertx, bridgeOptions);
        String host = config().getString("amqp.host");
        int port = config().getInteger("amqp.port");
        String username = config().getString("amqp.user", "anonymous");
        String password = config().getString("amqp.password", "anonymous");
        bridge.start(host, port, username, password, ar -> {
            if (ar.failed()) {
                log.warn("Bridge startup failed");
                startFuture.fail(ar.cause());
            } else {
                log.info("AMQP bridge to " + host + ":" + port + " started");
                bridgeStarted();
                startFuture.complete();
            }
        });
        tracer = GlobalTracer.get();
    }

    private void bridgeStarted() {
        MessageConsumer<JsonObject> consumer = bridge.<JsonObject>createConsumer(config().getString("amqp.consumer.driver-command"))
                .exceptionHandler(this::handleExceptions);
        consumer.handler(this::handleMessage);
    }

    private void handleMessage(Message<JsonObject> msg) {
        JsonObject msgBody = msg.body();
        JsonObject message = null;
        if (AmqpConstants.BODY_TYPE_DATA.equals(msgBody.getString(AmqpConstants.BODY_TYPE))) {
            try {
                message = new JsonObject(new String(msgBody.getBinary(AmqpConstants.BODY, new byte[]{}), "UTF-8"));
            } catch (UnsupportedEncodingException ignore) {
            }
        } else if (AmqpConstants.BODY_TYPE_VALUE.equals(msgBody.getString(AmqpConstants.BODY_TYPE))) {
            message = new JsonObject(msgBody.getString("body"));
        } else {
            log.warn("Unsupported AMQP Message Type " + msgBody.getString(AmqpConstants.BODY_TYPE) + ". Ignoring message");
            return;
        }
        if (message == null || message.isEmpty()) {
            log.warn("Message " + msgBody.toString() + " has no contents. Ignoring message");
            return;
        }
        String messageType = message.getString("messageType");
        if (!("AssignDriverCommand".equals(messageType))) {
            log.debug("Unexpected message type '" + messageType + "' in message " + message + ". Ignoring message");
            return;
        }
        log.debug("Consumed 'AssignedDriverCommand' message for ride " + message.getJsonObject("payload").getString("rideId"));

        //tracing
        Scope scope = TracingUtils.buildFollowingSpan(msgBody, tracer);

        // send message to producer verticle
        try {
            vertx.eventBus().<JsonObject>send("message-producer", message, TracingUtils.injectSpan(new DeliveryOptions(), tracer));
        } finally {
            if (scope != null) {
                scope.close();
            }
        }
    }

    private void handleExceptions(Throwable t) {
        log.error("Exception on AMQP consumer", t);
    }

    @Override
    public void stop(Future<Void> stopFuture) throws Exception {
        if (bridge != null) {
            bridge.close(ar -> {});
        }
        stopFuture.complete();
    }
}
