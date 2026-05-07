/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.metrics.prometheus;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.strimzi.kafka.metrics.prometheus.http.HttpServers;
import io.strimzi.kafka.metrics.prometheus.http.Listener;
import io.strimzi.kafka.metrics.prometheus.http.SslUtil;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

/**
 * Configuration for {@link ClientMetricsReporter}.
 */
public class ClientMetricsReporterConfig extends AbstractConfig {

    private static final Logger LOG = LoggerFactory.getLogger(ClientMetricsReporterConfig.class);
    private static final String CONFIG_PREFIX = "prometheus.metrics.reporter.";

    /**
     * Configuration key for the listener to expose the metrics.
     */
    public static final String LISTENER_CONFIG = CONFIG_PREFIX + "listener";

    /**
     * Default value for the listener configuration.
     */
    public static final String LISTENER_CONFIG_DEFAULT = "http://:8080";
    private static final String LISTENER_CONFIG_DOC = "The HTTP listener to expose the metrics.";

    /**
     * Configuration key to determine if the listener is enabled or not.
     */
    public static final String LISTENER_ENABLE_CONFIG = CONFIG_PREFIX + "listener.enable";

    /**
     * Default value for the listener enabled configuration.
     */
    public static final Boolean LISTENER_ENABLE_CONFIG_DEFAULT = true;
    private static final String LISTENER_ENABLE_CONFIG_DOC = "Enable the listener to expose the metrics.";

    /**
     * Configuration key for the ssl certificate PEM file path.
     */
    public static final String SSL_CERT_LOCATION_CONFIG = LISTENER_CONFIG + ".ssl.certificate.location";
    public static final String SSL_CERT_LOCATION_CONFIG_DOC = "The path to the PEM file containing the server certificate or certificate chain."
            + " If both file and inline certificate are provided, the inline certificate takes precedence.";

    /**
     * Configuration key for the ssl private key PEM file path.
     */
    public static final String SSL_KEY_LOCATION_CONFIG = LISTENER_CONFIG + ".ssl.key.location";
    public static final String SSL_KEY_LOCATION_CONFIG_DOC = "The path to the PEM file containing the private key."
            + " If both file and inline key are provided, the inline key takes precedence.";

    /**
     * Configuration key for the inline ssl certificate (PEM format).
     */
    public static final String SSL_CERT_CONFIG = LISTENER_CONFIG + ".ssl.certificate";
    public static final String SSL_CERT_CONFIG_DOC = "The server certificate or certificate chain in PEM format as an inline string."
            + " This takes precedence over " + SSL_CERT_LOCATION_CONFIG + ".";

    /**
     * Configuration key for the inline ssl private key (PEM format).
     */
    public static final String SSL_KEY_CONFIG = LISTENER_CONFIG + ".ssl.key";
    public static final String SSL_KEY_CONFIG_DOC = "The private key in PEM format as an inline string."
            + " This takes precedence over " + SSL_KEY_LOCATION_CONFIG + ".";

    /**
     * Configuration key for enabled TLS protocols (comma-separated).
     */
    public static final String SSL_ENABLED_PROTOCOLS_CONFIG = LISTENER_CONFIG + ".ssl.enabled.protocols";
    public static final String SSL_ENABLED_PROTOCOLS_CONFIG_DOC = "Comma-separated list of enabled TLS protocols (e.g. TLSv1.2,TLSv1.3).";

    /**
     * Configuration key for enabled cipher suites (comma-separated).
     */
    public static final String SSL_ENABLED_CIPHER_SUITES_CONFIG = LISTENER_CONFIG + ".ssl.enabled.cipher.suites";
    public static final String SSL_ENABLED_CIPHER_SUITES_CONFIG_DOC = "Comma-separated list of cipher suites the server will support.";

    /**
     * Configuration key for the allowlist of metrics to collect.
     */
    public static final String ALLOWLIST_CONFIG = CONFIG_PREFIX + "allowlist";

    /**
     * Default value for the allowlist configuration.
     */
    public static final String ALLOWLIST_CONFIG_DEFAULT = ".*";
    private static final String ALLOWLIST_CONFIG_DOC = "A comma separated list of regex patterns to specify the metrics to collect.";

    static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(LISTENER_CONFIG, ConfigDef.Type.STRING, LISTENER_CONFIG_DEFAULT, new Listener.ListenerValidator(), ConfigDef.Importance.HIGH, LISTENER_CONFIG_DOC)
            .define(ALLOWLIST_CONFIG, ConfigDef.Type.LIST, ALLOWLIST_CONFIG_DEFAULT, ConfigDef.Importance.HIGH, ALLOWLIST_CONFIG_DOC)
            .define(LISTENER_ENABLE_CONFIG, ConfigDef.Type.BOOLEAN, LISTENER_ENABLE_CONFIG_DEFAULT, ConfigDef.Importance.HIGH, LISTENER_ENABLE_CONFIG_DOC)
            .define(SSL_CERT_LOCATION_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, SSL_CERT_LOCATION_CONFIG_DOC)
            .define(SSL_KEY_LOCATION_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, SSL_KEY_LOCATION_CONFIG_DOC)
            .define(SSL_CERT_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, SSL_CERT_CONFIG_DOC)
            .define(SSL_KEY_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, SSL_KEY_CONFIG_DOC)
            .define(SSL_ENABLED_PROTOCOLS_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, SSL_ENABLED_PROTOCOLS_CONFIG_DOC)
            .define(SSL_ENABLED_CIPHER_SUITES_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, SSL_ENABLED_CIPHER_SUITES_CONFIG_DOC);

    final Listener listener;
    final boolean listenerEnabled;
    final PrometheusRegistry registry;
    final Pattern allowlist;
    SslUtil.SslConfig sslConfig = null;

    /**
     * Constructor.
     *
     * @param props    the configuration properties.
     * @param registry the metrics registry
     */
    @SuppressFBWarnings("CT_CONSTRUCTOR_THROW")
    public ClientMetricsReporterConfig(Map<?, ?> props, PrometheusRegistry registry) {
        super(CONFIG_DEF, props);
        this.listener = Listener.parseListener(getString(LISTENER_CONFIG));
        this.allowlist = compileAllowlist(getList(ALLOWLIST_CONFIG));
        this.listenerEnabled = getBoolean(LISTENER_ENABLE_CONFIG);
        this.registry = registry;

        if (listener.secure) {
            sslConfig = SslUtil.SslConfig.fromConfig(this);
        }
    }

    /**
     * Check if a metric is allowed.
     *
     * @param name the name of the metric.
     * @return true if the metric is allowed, false otherwise.
     */
    public boolean isAllowed(String name) {
        return allowlist.matcher(name).matches();
    }

    /**
     * The configured allowlist.
     *
     * @return The Pattern for the allowlist
     */
    public Pattern allowlist() {
        return allowlist;
    }

    Pattern compileAllowlist(List<String> allowlist) {
        for (String entry : allowlist) {
            try {
                Pattern.compile(entry);
            } catch (PatternSyntaxException pse) {
                throw new ConfigException("Invalid regex pattern found in " + ALLOWLIST_CONFIG + ": " + entry);
            }
        }
        String joined = allowlist.stream().map(v -> "(" + v + ")").collect(Collectors.joining("|"));
        return Pattern.compile(joined);
    }

    /**
     * Gets the listener.
     *
     * @return the listener.
     */
    public String listener() {
        return listener.toString();
    }

    /**
     * Check whether the listener is enabled.
     *
     * @return {@code true} if the listener is enabled, {@code false} otherwise.
     */
    public boolean isListenerEnabled() {
        return listenerEnabled;
    }

    @Override
    public String toString() {
        return "ClientMetricsReporterConfig{" +
                ", listener=" + listener +
                ", listenerEnabled=" + listenerEnabled +
                ", allowlist=" + allowlist +
                '}';
    }

    /**
     * Start the HTTP server for exposing metrics.
     *
     * @return An optional ServerCounter instance if {@link #LISTENER_ENABLE_CONFIG} is enabled, otherwise empty.
     */
    public synchronized Optional<HttpServers.ServerCounter> startHttpServer() {
        if (!listenerEnabled) {
            LOG.info("HTTP server listener not enabled");
            return Optional.empty();
        }
        LOG.debug("SSL configuration for HTTP server: {}", sslConfig);
        HttpServers.ServerCounter server = HttpServers.getOrCreate(listener, registry);
        LOG.info("HTTP server listening on http://{}:{}", listener.host, server.port());
        return Optional.of(server);
    }
}
