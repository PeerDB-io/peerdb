package io.peerdb.flow.jvm.iceberg.catalog.io.mapper;

import com.google.common.collect.ImmutableMap;
import io.peerdb.flow.peers.IcebergS3IoConfig;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.iceberg.aws.s3.S3FileIOProperties;

import java.util.Map;

@ApplicationScoped
public class S3IOConfigMapper extends FileIOConfigMapper<IcebergS3IoConfig> {
    @Override
    protected Map<String, String> mapSpecific(IcebergS3IoConfig config) {
        var builder = ImmutableMap.<String, String>builder()
                .put(S3FileIOProperties.ACCESS_KEY_ID, config.getAccessKeyId())
                .put(S3FileIOProperties.SECRET_ACCESS_KEY, config.getSecretAccessKey());
        if (config.hasEndpoint()) {
            builder.put(S3FileIOProperties.ENDPOINT, config.getEndpoint());
        }
        if (config.hasPathStyleAccess()) {
            builder.put(S3FileIOProperties.PATH_STYLE_ACCESS, config.getPathStyleAccess());
        }
        return builder.build();
    }

    @Override
    public String implementationClass() {
        return "org.apache.iceberg.aws.s3.S3FileIO";
    }
}
