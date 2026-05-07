/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.metrics.prometheus.http;

import io.strimzi.kafka.metrics.prometheus.ClientMetricsReporterConfig;
import org.apache.kafka.common.config.AbstractConfig;

import javax.net.ssl.SSLContext;
import java.security.KeyStore;

/**
 * Utility class for SSL related operations.
 */
public class SslUtil {

    /**
     * Helper method to creates a Https configuration with a default {@link SSLContext}.
     */
    public static SSLContext createSslContext(SslConfig sslConfig) {
        try {
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, null, null);
            return sslContext;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create SSL context for HTTPS server", e);
        }
    }

    public static class SslConfig {
        public String certificatePem;
        public String privateKeyPem;
        public String certificatePath;
        public String privateKeyPath;
        public String enabledProtocols;
        public String enabledCipherSuites;

        private SslConfig(String certificatePem, String privateKeyPem, String certificatePath, String privateKeyPath,
                          String enabledProtocols, String enabledCipherSuites) {
            this.certificatePem = certificatePem;
            this.privateKeyPem = privateKeyPem;
            this.certificatePath = certificatePath;
            this.privateKeyPath = privateKeyPath;
            this.enabledProtocols = enabledProtocols;
            this.enabledCipherSuites = enabledCipherSuites;
        }

        public static SslConfig fromConfig(AbstractConfig config) {
            String certificatePem = null;
            String privateKeyPem = null;
            String certificatePath = null;
            String privateKeyPath = null;
            String enabledProtocols = null;
            String enabledCipherSuites = null;

            try {
                certificatePem = config.getString(ClientMetricsReporterConfig.SSL_CERT_CONFIG);
            } catch (Exception e) {
            }
            try {
                privateKeyPem = config.getString(ClientMetricsReporterConfig.SSL_KEY_CONFIG);
            } catch (Exception e) {
            }
            try {
                certificatePath = config.getString(ClientMetricsReporterConfig.SSL_CERT_LOCATION_CONFIG);
            } catch (Exception e) {
            }
            try {
                privateKeyPath = config.getString(ClientMetricsReporterConfig.SSL_KEY_LOCATION_CONFIG);
            } catch (Exception e) {
                privateKeyPath = null;
            }
            try {
                enabledProtocols = config.getString(ClientMetricsReporterConfig.SSL_ENABLED_PROTOCOLS_CONFIG);
            } catch (Exception e) {
                enabledProtocols = null;
            }
            try {
                enabledCipherSuites = config.getString(ClientMetricsReporterConfig.SSL_ENABLED_CIPHER_SUITES_CONFIG);
            } catch (Exception e) {
                enabledCipherSuites = null;
            }

            return new SslConfig(certificatePem, privateKeyPem, certificatePath, privateKeyPath,
                    enabledProtocols, enabledCipherSuites);
        }

        public KeyStore loadKeyStore() {
            try {
                KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
                return keyStore;
            } catch (Exception e) {
                throw new RuntimeException("Failed to load key store for HTTPS server", e);
            }
        }

        @Override
        public String toString() {
            return "SslConfig{" +
                    ", certificatePath='" + certificatePath + '\'' +
                    ", privateKeyPath='" + privateKeyPath + '\'' +
                    ", hasInlineCertificate='" + (certificatePem != null) + '\'' +
                    ", hasInlineKey='" + (privateKeyPem != null) + '\'' +
                    ", enabledProtocols='" + enabledProtocols + '\'' +
                    ", enabledCipherSuites='" + enabledCipherSuites + '\'' +
                    '}';
        }
    }
}
