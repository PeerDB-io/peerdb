package io.peerdb.flow.jvm.iceberg.catalog.io.mapper;

import com.google.common.collect.ImmutableMap;
import io.peerdb.flow.peers.IcebergGCSIoConfig;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.Map;

@ApplicationScoped
public class GCSIOConfigMapper extends FileIOConfigMapper<IcebergGCSIoConfig> {
    @Override
    protected Map<String, String> mapSpecific(IcebergGCSIoConfig config) {
        // TODO complete this
        var builder = ImmutableMap.<String, String>builder();
//                .put(GCPProperties.GCS_PROJECT_ID, config.getAccessKeyId())
//                .put(S3FileIOProperties.SECRET_ACCESS_KEY, config.getSecretAccessKey());
//        if (config.hasEndpoint()) {
//            builder.put(S3FileIOProperties.ENDPOINT, config.getEndpoint());
//        }
//        if (config.hasPathStyleAccess()) {
//            builder.put(S3FileIOProperties.PATH_STYLE_ACCESS, config.getPathStyleAccess());
//        }
        return builder.build();
    }

    @Override
    public String implementationClass() {
        return "org.apache.iceberg.gcp.gcs.GCSFileIO";
    }
}
