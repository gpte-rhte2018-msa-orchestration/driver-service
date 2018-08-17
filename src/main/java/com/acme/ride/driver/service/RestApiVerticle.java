package com.acme.ride.driver.service;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.ext.healthchecks.HealthCheckHandler;
import io.vertx.ext.healthchecks.Status;
import io.vertx.ext.web.Router;

public class RestApiVerticle extends AbstractVerticle {

    @SuppressWarnings("unchecked")
    @Override
    public void start(Future<Void> startFuture) throws Exception {

        Router router = Router.router(vertx);

        HealthCheckHandler healthCheckHandler = HealthCheckHandler.create(vertx)
                .register("health", this::health);
        router.get("/health").handler(healthCheckHandler);

        vertx.createHttpServer()
        .requestHandler(router::accept)
        .listen(config().getInteger("http.port", 8080), ar -> {
            if (ar.succeeded()) {
                startFuture.complete();
            } else {
                startFuture.fail(ar.cause());
            }
        });
    }

    private void health(Future<Status> future) {
        future.complete(Status.OK());
    }

    @Override
    public void stop(Future<Void> stopFuture) throws Exception {
        stopFuture.complete();
    }
}
