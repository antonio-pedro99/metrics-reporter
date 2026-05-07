/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.metrics.prometheus.http;

import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsParameters;

import javax.net.ssl.SSLContext;

/**
 * Custom HTTPS configurator to be used with the HTTP server.
 */
public class CustomHttpsConfigurator extends HttpsConfigurator {

    /**
     * Creates a Https configuration, with the given {@link SSLContext}.
     *
     * @throws NullPointerException if no {@code SSLContext} supplied
     */
    public CustomHttpsConfigurator(SSLContext context) {
        super(context);
    }

    @Override
    public void configure(HttpsParameters params) {
        super.configure(params);
    }
}
