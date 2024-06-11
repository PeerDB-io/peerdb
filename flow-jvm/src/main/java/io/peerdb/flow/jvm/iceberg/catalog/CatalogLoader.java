package io.peerdb.flow.jvm.iceberg.catalog;


import io.peerdb.flow.jvm.iceberg.catalog.io.mapper.GCSIOConfigMapper;
import io.peerdb.flow.jvm.iceberg.catalog.io.mapper.S3IOConfigMapper;
import io.peerdb.flow.jvm.iceberg.catalog.mapper.HiveConfigMapper;
import io.peerdb.flow.jvm.iceberg.catalog.mapper.JdbcCatalogMapper;
import io.peerdb.flow.peers.IcebergCatalog;
import io.quarkus.logging.Log;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;

import java.util.Collections;


@Singleton
public class CatalogLoader {
    @Inject
    HiveConfigMapper hiveConfigMapper;

    @Inject
    JdbcCatalogMapper jdbcCatalogMapper;

    @Inject
    S3IOConfigMapper s3IOConfigMapper;

    @Inject
    GCSIOConfigMapper gcsIOConfigMapper;


    public Catalog loadCatalog(IcebergCatalog icebergCatalogConfig) {
        var icebergIOConfig = icebergCatalogConfig.getIoConfig();
        var fileIoConfig = switch (icebergIOConfig.getConfigCase()) {
            case S3 -> s3IOConfigMapper.map(icebergIOConfig.getS3());
            case GCS -> gcsIOConfigMapper.map(icebergIOConfig.getGcs());
            default -> {
                Log.errorf("Unexpected value for file io config: %s", icebergIOConfig.getConfigCase());
                yield Collections.<String, String>emptyMap();
            }
        };

        var catalogConfig = switch (icebergCatalogConfig.getConfigCase()) {
            case HIVE ->
                    hiveConfigMapper.map(icebergCatalogConfig.getCommonConfig(), icebergCatalogConfig.getHive(), fileIoConfig);
            case JDBC ->
                    jdbcCatalogMapper.map(icebergCatalogConfig.getCommonConfig(), icebergCatalogConfig.getJdbc(), fileIoConfig);
            default ->
                    throw new IllegalArgumentException("Unexpected value for catalog config: " + icebergCatalogConfig.getConfigCase());
        };
        // TODO look at hadoop
        return CatalogUtil.buildIcebergCatalog(icebergCatalogConfig.getCommonConfig().getName(), catalogConfig, null);
    }
}
