/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.metrics.prometheus.http;

import io.strimzi.kafka.metrics.prometheus.ClientMetricsReporterConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.strimzi.kafka.metrics.prometheus.ClientMetricsReporterConfig.LISTENER_CONFIG;

/**
 * Class parsing and handling the listener specified via {@link ClientMetricsReporterConfig#LISTENER_CONFIG} for
 * the HTTP server used to expose the metrics.
 */
public class Listener {

    private static final Pattern PATTERN = Pattern.compile("https?://\\[?([0-9a-zA-Z\\-%._:]*)]?:([0-9]+)");

    /**
     * The host of the listener
     */
    public final String host;
    /**
     * The port of the listener
     */
    public final int port;
    /**
     * Whether the listener is secure (HTTPS) or not (HTTP)
     */
    public final boolean secure;
    /**
     * The scheme of the listener. Default is "http"
     */
    public String scheme;

    /* test */ Listener(String host, int port, boolean secure) {
        this.host = host;
        this.port = port;
        this.secure = secure;
        this.scheme = secure ? "https" : "http";
    }

    /**
     * Build a Listener instance from a "http://[host]:[port]" string
     * @param listener the input string
     * @return the listener
     */
    public static Listener parseListener(String listener) {
        Matcher matcher = PATTERN.matcher(listener);
        if (matcher.matches()) {
            String host = matcher.group(1);
            int port = Integer.parseInt(matcher.group(2));
            return new Listener(host, port, listener.startsWith("https"));
        } else {
            throw new ConfigException(LISTENER_CONFIG, listener, "Listener must be of format http://[host]:[port]");
        }
    }

    @Override
    public String toString() {
        return scheme + "://" + host + ":" + port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Listener listener = (Listener) o;
        return port == listener.port && Objects.equals(host, listener.host) && secure == listener.secure;
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port, secure);
    }

    /**
     * Validator to check the user provided listener configuration
     */
    public static class ListenerValidator implements ConfigDef.Validator {

        /**
         * Empty constructor
         */
        public ListenerValidator() { }

        @Override
        public void ensureValid(String name, Object value) {
            Matcher matcher = PATTERN.matcher(String.valueOf(value));
            if (!matcher.matches()) {
                throw new ConfigException(name, value, "The Listener must be of format http(s)://[host]:[port]");
            }
        }
    }
}
