package com.acme.ride.driver.service;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.UUID;
import javax.jms.CompletionListener;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.TextMessage;

import com.acme.ride.driver.service.tracing.TracingUtils;
import io.opentracing.Tracer;
import io.opentracing.contrib.jms2.TracingMessageProducer;
import io.opentracing.util.GlobalTracer;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.messaginghub.pooled.jms.JmsPoolConnectionFactory;

public class MessageProducerVerticle extends AbstractVerticle {

    private final static Logger log = LoggerFactory.getLogger("MessageProducer");

    private ConnectionFactory connectionFactory;

    private Tracer tracer;

    private int minDelayBeforeDriverAssignedEvent;

    private int maxDelayBeforeDriverAssignedEvent;

    private int minDelayBeforeRideStartedEvent;

    private int maxDelayBeforeRideStartedEvent;

    private int minDelayBeforeRideEndedEvent;

    private int maxDelayBeforeRideEndedEvent;

    @Override
    public void start(Future<Void> startFuture) throws Exception {

        connectionFactory = createPooledConnectionFactory();

        minDelayBeforeDriverAssignedEvent = config().getInteger("driver.assigned.min.delay", 1);
        maxDelayBeforeDriverAssignedEvent = config().getInteger("driver.assigned.max.delay", 3);
        minDelayBeforeRideStartedEvent = config().getInteger("ride.started.min.delay", 5);
        maxDelayBeforeRideStartedEvent = config().getInteger("ride.started.max.delay", 10);
        minDelayBeforeRideEndedEvent = config().getInteger("ride.ended.min.delay", 5);
        maxDelayBeforeRideEndedEvent = config().getInteger("ride.ended.max.delay", 10);

        tracer = GlobalTracer.get();

        vertx.eventBus().consumer("message-producer", this::handleMessage);

        startFuture.complete();
    }

    private ConnectionFactory createPooledConnectionFactory() {
        JmsPoolConnectionFactory factory = new JmsPoolConnectionFactory();
        factory.setConnectionFactory(createConnectionFactory());
        factory.setExplicitProducerCacheSize(config().getInteger("amqp.pool.explicit-producer-cache-size"));
        factory.setUseAnonymousProducers(config().getBoolean("amqp.pool.use-anonymous-producers"));
        return factory;
    }

    private JmsConnectionFactory createConnectionFactory() {

        JmsConnectionFactory factory = new JmsConnectionFactory();
        factory.setRemoteURI(config().getString("amqp.protocol") + "://" + config().getString("amqp.host")
            + ":" + config().getInteger("amqp.port") + "?" + config().getString("amqp.query"));
        factory.setUsername(config().getString("amqp.user"));
        factory.setPassword(config().getString("amqp.password"));
        return factory;
    }

    private Session session() throws JMSException {
        Connection connection = connectionFactory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        return session;
    }

    private Destination resolveDestination(Session session, String destination) throws JMSException {
        return session.createTopic(destination);
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
            doSendDriverAssignedMessage(msgIn, driverId);
            sendRideStartedEventMessage(msgIn);
        });
    }

    private void sendRideStartedEventMessage(final Message<JsonObject> msgIn) {

        if (dropRideStartedEvent(getRideId(msgIn.body()))) {
            return;
        }
        vertx.setTimer(delayBeforeMessage(minDelayBeforeRideStartedEvent, maxDelayBeforeRideStartedEvent), i -> {
            doSendRideStartedEventMessage(msgIn);
            sendRideEndedEventMessage(msgIn);
        });
    }

    private void sendRideEndedEventMessage(final Message<JsonObject> msgIn) {
        vertx.setTimer(delayBeforeMessage(minDelayBeforeRideEndedEvent, maxDelayBeforeRideEndedEvent), i -> {
            doSendRideEndedEventMessage(msgIn);
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

    private void doSendDriverAssignedMessage(Message<JsonObject> msgIn, String driverId) {
        JsonObject msgOut = new JsonObject();
        String messageType = "DriverAssignedEvent";
        msgOut.put("messageType", messageType);
        msgOut.put("id", UUID.randomUUID().toString());
        msgOut.put("traceId", msgIn.body().getString("traceId"));
        msgOut.put("sender", "DriverServiceSimulator");
        msgOut.put("timestamp", Instant.now().toEpochMilli());
        JsonObject payload = new JsonObject();
        String rideId = msgIn.body().getJsonObject("payload").getString("rideId");
        payload.put("rideId", rideId);
        payload.put("driverId", driverId);
        msgOut.put("payload", payload);

        sendMessageToTopic(msgOut, config().getString("amqp.producer.driver-event"), msgIn);
    }

    private void doSendRideStartedEventMessage(Message<JsonObject> msgIn) {
        JsonObject msgOut = new JsonObject();
        String messageType = "RideStartedEvent";
        msgOut.put("messageType", messageType);
        msgOut.put("id", UUID.randomUUID().toString());
        msgOut.put("traceId", msgIn.body().getString("traceId"));
        msgOut.put("sender", "DriverServiceSimulator");
        msgOut.put("timestamp", Instant.now().toEpochMilli());
        JsonObject payload = new JsonObject();
        String rideId = msgIn.body().getJsonObject("payload").getString("rideId");
        payload.put("rideId", rideId);
        payload.put("timestamp", Instant.now().toEpochMilli());
        msgOut.put("payload", payload);

        sendMessageToTopic(msgOut, config().getString("amqp.producer.ride-event"), msgIn);
    }

    private void doSendRideEndedEventMessage(Message<JsonObject> msgIn) {
        JsonObject msgOut = new JsonObject();
        String messageType = "RideEndedEvent";
        msgOut.put("messageType", messageType);
        msgOut.put("id", UUID.randomUUID().toString());
        msgOut.put("traceId", msgIn.body().getString("traceId"));
        msgOut.put("sender", "DriverServiceSimulator");
        msgOut.put("timestamp", Instant.now().toEpochMilli());
        JsonObject payload = new JsonObject();
        String rideId = msgIn.body().getJsonObject("payload").getString("rideId");
        payload.put("rideId", rideId);
        payload.put("timestamp", Instant.now().toEpochMilli());
        msgOut.put("payload", payload);

        sendMessageToTopic(msgOut, config().getString("amqp.producer.ride-event"), msgIn);
    }

    private void sendMessageToTopic(JsonObject msg, String destination, Message<JsonObject> msgIn) {
        try {
            final Context context = vertx.getOrCreateContext();
            Session session = session();
            Destination topic = resolveDestination(session, destination);
            TextMessage message = session.createTextMessage(msg.toString());
            javax.jms.MessageProducer producer = session.createProducer(topic);
            TracingMessageProducer tracingMessageProducer = new TracingMessageProducer(producer, tracer);

            TracingUtils.injectParentSpan(msgIn, message, tracer);
            tracingMessageProducer.send(message, new MyCompletionListener(context, msg));
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    private String getRideId(JsonObject message) {
        return message.getJsonObject("payload").getString("rideId");
    }

    private void handleExceptions(Throwable t) {
        log.error("Exception on AMQP Producer", t);
    }

    @Override
    public void stop(Future<Void> stopFuture) throws Exception {
        ((JmsPoolConnectionFactory) connectionFactory).stop();
        stopFuture.complete();
    }

    private static class MyCompletionListener implements CompletionListener {

        private Context context;
        private String type;
        private String rideId;

        private MyCompletionListener(Context context, JsonObject msg) {
            this.context = context;
            this.type = msg.getString("messageType");
            this.rideId = msg.getJsonObject("payload").getString("rideId");
        }

        @Override
        public void onCompletion(javax.jms.Message message) {
            context.runOnContext(v -> {
                log.debug("Sent " + type + " for ride " + rideId);
            });
        }

        @Override
        public void onException(javax.jms.Message message, Exception exception) {
            context.runOnContext(v -> {
                log.error("Exception sending " + type + " for ride " + rideId, exception);
            });
        }
    }
}
