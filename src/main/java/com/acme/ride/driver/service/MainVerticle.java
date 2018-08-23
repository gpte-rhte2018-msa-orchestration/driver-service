package com.acme.ride.driver.service;

import io.jaegertracing.Configuration;
import io.opentracing.util.GlobalTracer;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class MainVerticle extends AbstractVerticle {

    private static final Logger log = LoggerFactory.getLogger(MainVerticle.class);

    @Override
    public void start(Future<Void> startFuture) {
        ConfigStoreOptions jsonConfigStore = new ConfigStoreOptions().setType("json");
        ConfigStoreOptions appStore = new ConfigStoreOptions()
            .setType("configmap")
            .setFormat("yaml")
            .setConfig(new JsonObject()
                .put("name", System.getProperty("application.configmap", "driver-service"))
                .put("key", "application-config.yaml"));

        ConfigRetrieverOptions options = new ConfigRetrieverOptions();
        if (System.getenv("KUBERNETES_NAMESPACE") != null) {
            //we're running in Kubernetes
            options.addStore(appStore);
        } else {
            //default to json based config
            jsonConfigStore.setConfig(config());
            options.addStore(jsonConfigStore);
        }

        ConfigRetriever.create(vertx, options)
        .getConfig(ar -> {
            if (ar.succeeded()) {
                deployVerticles(ar.result(), startFuture);
            } else {
                log.error("Failed to retrieve the configuration.");
                startFuture.fail(ar.cause());
            }
        });
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void deployVerticles(JsonObject config, Future<Void> startFuture) {

        initTracer(config);

        Future<String> restApiVerticleFuture = Future.future();
        Future<String> messageConsumerVerticleFuture = Future.future();
        Future<String> messageProducerVerticleFuture = Future.future();
        DeploymentOptions options = new DeploymentOptions().setConfig(config);
        vertx.deployVerticle(new RestApiVerticle(), options, restApiVerticleFuture.completer());
        vertx.deployVerticle(new MessageConsumerVerticle(), options, messageConsumerVerticleFuture.completer());
        vertx.deployVerticle(new MessageProducerVerticle(), options, messageProducerVerticleFuture.completer());
        CompositeFuture.all(restApiVerticleFuture, messageConsumerVerticleFuture, messageProducerVerticleFuture).setHandler(ar -> {
            if (ar.succeeded()) {
                log.info("Verticles deployed successfully.");
                startFuture.complete();
            } else {
                log.fatal("Verticles NOT deployed successfully.");
                startFuture.fail(ar.cause());
            }
        });
    }

    private void initTracer(JsonObject config) {
        String serviceName = config.getString("service-name");
        if (serviceName == null || serviceName.isEmpty()) {
            log.info("No Service Name set. Skipping initialization of the Jaeger Tracer.");
            return;
        }

        Configuration configuration = new Configuration(serviceName)
                .withSampler(new Configuration.SamplerConfiguration()
                        .withType(config.getString("sampler-type"))
                        .withParam(getPropertyAsNumber(config, "sampler-param"))
                        .withManagerHostPort(config.getString("sampler-manager-host-port")))
                .withReporter(new Configuration.ReporterConfiguration()
                        .withLogSpans(config.getBoolean("reporter-log-spans"))
                        .withFlushInterval(config.getInteger("reporter-flush-interval"))
                        .withMaxQueueSize(config.getInteger("reporter-flush-interval"))
                        .withSender(new Configuration.SenderConfiguration()
                                .withAgentHost(config.getString("agent-host"))
                                .withAgentPort(config.getInteger("agent-port"))));
        GlobalTracer.register(configuration.getTracer());
    }

    private Number getPropertyAsNumber(JsonObject json, String key) {
        Object o  = json.getValue(key);
        if (o instanceof Number) {
            return (Number) o;
        }
        return null;
    }

    @Override
    public void stop(Future<Void> stopFuture) throws Exception {
        super.stop(stopFuture);
    }
}
