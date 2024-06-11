package io.peerdb.flow.jvm.iceberg.catalog.mapper;

import io.peerdb.flow.peers.HiveIcebergCatalog;
import jakarta.inject.Singleton;
import org.apache.iceberg.CatalogUtil;

import java.util.Collections;
import java.util.Map;

@Singleton
public class HiveConfigMapper extends CatalogConfigMapper<HiveIcebergCatalog> {
    @Override
    protected Map<String, String> mapSpecific(HiveIcebergCatalog config) {
        // TODO complete this
        return Collections.emptyMap();
    }

    @Override
    public String implementationClass() {
        return CatalogUtil.ICEBERG_CATALOG_HIVE;
    }
}
