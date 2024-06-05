package io.peerdb.flow.jvm.iceberg.catalog.mapper;


import com.google.common.collect.ImmutableMap;
import io.peerdb.flow.peers.CommonIcebergCatalog;
import io.quarkus.logging.Log;
import org.apache.iceberg.CatalogProperties;

import java.util.HashMap;
import java.util.Map;

public abstract class CatalogConfigMapper<T> {
    protected Map<String, String> mapCommon(CommonIcebergCatalog config) {
        var builder = ImmutableMap.<String, String>builder()
                .put(CatalogProperties.URI, config.getUri())
                .put(CatalogProperties.WAREHOUSE_LOCATION, config.getWarehouseLocation())
                .put(CatalogProperties.CATALOG_IMPL, this.implementationClass());
        if (config.hasClientPoolSize()) {
            builder.put(CatalogProperties.CLIENT_POOL_SIZE, String.valueOf(config.getClientPoolSize()));
        }
        if (config.hasCacheEnabled()) {
                builder.put(CatalogProperties.CACHE_ENABLED, String.valueOf(config.getCacheEnabled()));
        }
        builder.putAll(config.getAdditionalPropertiesMap());
        return builder.build();
    }

    public Map<String, String> map(CommonIcebergCatalog commonConfig, T config, Map<String, String> fileIoConfig) {
        var map = new HashMap<>(mapCommon(commonConfig));
        map.putAll(this.mapSpecific(config));
        map.putAll(fileIoConfig);
        Log.infof("Mapped catalog config: %s", map);
        return map;
    }


    protected abstract Map<String, String> mapSpecific(T config);

    public abstract String implementationClass();
}
