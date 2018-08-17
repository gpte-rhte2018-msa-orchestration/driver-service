package com.acme.ride.driver.service;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.UUID;

import io.vertx.amqpbridge.AmqpBridge;
import io.vertx.amqpbridge.AmqpBridgeOptions;
import io.vertx.amqpbridge.AmqpConstants;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.JksOptions;

public class MessageProducerVerticle extends AbstractVerticle {

    private final static Logger log = LoggerFactory.getLogger("MessageProducer");

    private AmqpBridge bridge;

    private MessageProducer<JsonObject> driverEventProducer;

    private MessageProducer<JsonObject> rideEventProducer;

    private int minDelayBeforeDriverAssignedEvent = 1;

    private int maxDelayBeforeDriverAssignedEvent = 3;

    private int minDelayBeforeRideStartedEvent = 1;

    private int maxDelayBeforeRideStartedEvent = 3;

    private int minDelayBeforeRideEndedEvent = 1;

    private int maxDelayBeforeRideEndedEvent = 3;

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        AmqpBridgeOptions bridgeOptions = new AmqpBridgeOptions();
        bridgeOptions.setConnectTimeout(10000);
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
    }

    private void bridgeStarted() {
        driverEventProducer = bridge.<JsonObject>createProducer(config().getString("amqp.producer.driver-event")).exceptionHandler(this::handleExceptions);
        rideEventProducer = bridge.<JsonObject>createProducer(config().getString("amqp.producer.ride-event")).exceptionHandler(this::handleExceptions);
        vertx.eventBus().consumer("message-producer", this::handleMessage);
    }

    private void handleMessage(Message<JsonObject> msg) {

        if (dropAssignDriverEvent(getRideId(msg.body()))) {
            msg.reply(null);
            return;
        }
        sendDriverAssignedMessage(msg, getDriverId());
    }

    private boolean dropAssignDriverEvent(String rideId) {
        try {
            UUID uuid = UUID.fromString(rideId);
            long leastSignificantBits = uuid.getLeastSignificantBits() & 0x0000000F;
            if (leastSignificantBits == 15) {
                log.info("Dropping 'AssignedDriverEvent' for rideId " + rideId);
                return true;
            }
        } catch (IllegalArgumentException e) {
            // rideId is not an UUID
            return false;
        }
        return false;
    }

    private boolean dropRideStartedEvent(String rideId) {
        try {
            UUID uuid = UUID.fromString(rideId);
            long leastSignificantBits = uuid.getLeastSignificantBits() & 0x0000000F;
            if (leastSignificantBits == 14) {
                log.info("Dropping 'RideStartedEvent' for rideId " + rideId);
                return true;
            }
        } catch (IllegalArgumentException e) {
            // rideId is not an UUID
            return false;
        }
        return false;
    }

    private String getDriverId() {
        return "driver" + randomInteger(100, 200).intValue();
    }

    private void sendDriverAssignedMessage(final Message<JsonObject> msgIn, final String driverId) {
        vertx.setTimer(delayBeforeMessage(minDelayBeforeDriverAssignedEvent, maxDelayBeforeDriverAssignedEvent), i -> {
            doSendDriverAssignedMessage(msgIn.body(), driverId);
            sendRideStartedEventMessage(msgIn);
        });
    }

    private void sendRideStartedEventMessage(final Message<JsonObject> msgIn) {
        if (dropRideStartedEvent(getRideId(msgIn.body()))) {
            msgIn.reply(null);
            return;
        }
        vertx.setTimer(delayBeforeMessage(minDelayBeforeRideStartedEvent, maxDelayBeforeRideStartedEvent), i -> {
            doSendRideStartedEventMessage(msgIn.body());
            sendRideEndedEventMessage(msgIn);
        });
    }

    private void sendRideEndedEventMessage(final Message<JsonObject> msgIn) {
        vertx.setTimer(delayBeforeMessage(minDelayBeforeRideEndedEvent, maxDelayBeforeRideEndedEvent), i -> {
            doSendRideEndedEventMessage(msgIn.body());
            msgIn.reply(null);
        });
    }

    private int delayBeforeMessage(int min, int max) {
        return randomInteger(min, max).intValue()*1000;
    }

    private BigInteger randomInteger(int min, int max) {
        BigInteger s = BigInteger.valueOf(min);
        BigInteger e = BigInteger.valueOf(max);

        return s.add(new BigDecimal(Math.random()).multiply(new BigDecimal(e.subtract(s))).toBigInteger());
    }

    private void doSendDriverAssignedMessage(JsonObject msgIn, String driverId) {
        JsonObject msgOut = new JsonObject();
        msgOut.put("messageType","DriverAssignedEvent");
        msgOut.put("id", UUID.randomUUID().toString());
        msgOut.put("traceId", msgIn.getString("traceId"));
        msgOut.put("sender", "DriverServiceSimulator");
        msgOut.put("timestamp", Instant.now().toEpochMilli());
        JsonObject payload = new JsonObject();
        payload.put("rideId", msgIn.getJsonObject("payload").getString("rideId"));
        payload.put("driverId", driverId);
        msgOut.put("payload", payload);

        sendMessageToTopic(msgOut, driverEventProducer);
    }

    private void doSendRideStartedEventMessage(JsonObject msgIn) {
        JsonObject msgOut = new JsonObject();
        msgOut.put("messageType","RideStartedEvent");
        msgOut.put("id", UUID.randomUUID().toString());
        msgOut.put("traceId", msgIn.getString("traceId"));
        msgOut.put("sender", "DriverServiceSimulator");
        msgOut.put("timestamp", Instant.now().toEpochMilli());
        JsonObject payload = new JsonObject();
        payload.put("rideId", msgIn.getJsonObject("payload").getString("rideId"));
        payload.put("timestamp", Instant.now().toEpochMilli());
        msgOut.put("payload", payload);

        sendMessageToTopic(msgOut, rideEventProducer);
    }

    private void doSendRideEndedEventMessage(JsonObject msgIn) {
        JsonObject msgOut = new JsonObject();
        msgOut.put("messageType","RideEndedEvent");
        msgOut.put("id", UUID.randomUUID().toString());
        msgOut.put("traceId", msgIn.getString("traceId"));
        msgOut.put("sender", "DriverServiceSimulator");
        msgOut.put("timestamp", Instant.now().toEpochMilli());
        JsonObject payload = new JsonObject();
        payload.put("rideId", msgIn.getJsonObject("payload").getString("rideId"));
        payload.put("timestamp", Instant.now().toEpochMilli());
        msgOut.put("payload", payload);

        sendMessageToTopic(msgOut, rideEventProducer);
    }

    private void sendMessageToTopic(JsonObject msg, MessageProducer<JsonObject> messageProducer) {
        JsonObject amqpMsg = new JsonObject();
        amqpMsg.put(AmqpConstants.BODY_TYPE, AmqpConstants.BODY_TYPE_VALUE);
        amqpMsg.put(AmqpConstants.BODY, msg.toString());
        JsonObject annotations = new JsonObject();
        byte b = 5;
        annotations.put("x-opt-jms-msg-type", b);
        amqpMsg.put(AmqpConstants.MESSAGE_ANNOTATIONS, annotations);
        messageProducer.send(amqpMsg);
    }

    private String getRideId(JsonObject message) {
        return message.getJsonObject("payload").getString("rideId");
    }

    private void handleExceptions(Throwable t) {
        t.printStackTrace();
    }

    @Override
    public void stop(Future<Void> stopFuture) throws Exception {
        if (driverEventProducer != null) {
            driverEventProducer.close();
        }
        if (rideEventProducer != null) {
            rideEventProducer.close();
        }
        if (bridge != null) {
            bridge.close(ar -> {});
        }
        stopFuture.complete();
    }
}
