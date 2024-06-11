package io.peerdb.flow.jvm.iceberg.catalog.mapper;

import io.peerdb.flow.peers.JdbcIcebergCatalog;
import jakarta.inject.Singleton;
import org.apache.iceberg.jdbc.JdbcCatalog;

import java.util.Map;
import java.util.stream.Collectors;


@Singleton
public class JdbcCatalogMapper extends CatalogConfigMapper<JdbcIcebergCatalog> {

    @Override
    protected Map<String, String> mapSpecific(JdbcIcebergCatalog config) {
        return Map.of(
                "user", config.getUser(),
                "password", config.getPassword(),
                "useSSL", config.hasUseSsl() ? String.valueOf(config.getUseSsl()) : "true",
                "verifyServerCertificate", config.hasVerifyServerCertificate() ? String.valueOf(config.getVerifyServerCertificate()) : "false"

        ).entrySet().stream().collect(Collectors.toMap(e -> JdbcCatalog.PROPERTY_PREFIX + e.getKey(), Map.Entry::getValue));
    }

    @Override
    public String implementationClass() {
        return JdbcCatalog.class.getName();
    }
}
