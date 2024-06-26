package io.peerdb.flow.jvm.iceberg.catalog.mapper;

import com.google.common.collect.ImmutableMap;
import io.peerdb.flow.peers.HiveIcebergCatalog;
import jakarta.inject.Singleton;
import org.apache.iceberg.CatalogUtil;

import java.util.Map;


/**
 * This requires the underlying thrift connection like thrift://localhost:9083
 */

@Singleton
public class HiveConfigMapper extends CatalogConfigMapper<HiveIcebergCatalog> {
    @Override
    protected Map<String, String> mapSpecific(HiveIcebergCatalog config) {
        return ImmutableMap.<String, String>builder()
                // TODO add these if needed
//                .put(HiveCatalog.HMS_DB_OWNER, "hive")
                .build();
    }

    @Override
    public String implementationClass() {
        return CatalogUtil.ICEBERG_CATALOG_HIVE;
    }
}
