package com.acme.ride.driver.service;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.messaginghub.pooled.jms.JmsPoolConnectionFactory;

public class MessageConsumerVerticle extends AbstractVerticle {

    private final static Logger log = LoggerFactory.getLogger("MessageConsumer");

    private ConnectionFactory connectionFactory;

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        connectionFactory = createPooledConnectionFactory();
        try {
            setupConsumer();
            startFuture.complete();
        } catch (JMSException e) {
            startFuture.fail(e);
        }
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

    private void setupConsumer() throws JMSException {
            Connection connection = connectionFactory.createConnection();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic topic = session.createTopic(config().getString("amqp.consumer.driver-command"));
            MessageConsumer consumer = session.createSharedDurableConsumer(topic, config().getString("amqp.subscription.driver-command"));
            final Context context = vertx.getOrCreateContext();
            consumer.setMessageListener(message -> context.runOnContext(v -> {
                try {
                    if (!(message instanceof TextMessage)) {
                        log.warn("Unexpected Message Type - ignoring:" + message.getClass().getName());
                        return;
                    }
                    JsonObject msgBody = new JsonObject(((TextMessage)message).getText());
                    String messageType = msgBody.getString("messageType");
                    if (!("AssignDriverCommand".equals(messageType))) {
                        log.debug("Unexpected message type '" + messageType + "' in message " + msgBody + ". Ignoring message");
                        return;
                    }
                    log.debug("Consumed 'AssignedDriverCommand' message for ride " + msgBody.getJsonObject("payload").getString("rideId"));
                    // send message to producer verticle
                    vertx.eventBus().<JsonObject>send("message-producer", msgBody);
                } catch (JMSException e) {
                    log.error("Exception when consuming message");
                }
            }));
            connection.start();
    }

    private void handleExceptions(Throwable t) {
        log.error("Exception on AMQP consumer", t);
    }

    @Override
    public void stop(Future<Void> stopFuture) throws Exception {
        ((JmsPoolConnectionFactory) connectionFactory).stop();
        stopFuture.complete();
    }
}
