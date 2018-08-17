package com.acme.ride.driver.service;

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

    @Override
    public void stop(Future<Void> stopFuture) throws Exception {
        super.stop(stopFuture);
    }
}
