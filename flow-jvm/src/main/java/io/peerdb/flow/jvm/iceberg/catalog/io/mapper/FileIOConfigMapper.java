package io.peerdb.flow.jvm.iceberg.catalog.io.mapper;


import io.quarkus.logging.Log;
import org.apache.iceberg.CatalogProperties;

import java.util.HashMap;
import java.util.Map;

public abstract class FileIOConfigMapper<T> {

    protected abstract Map<String, String> mapSpecific(T config);

    protected Map<String, String> mapCommon() {
        return Map.of(
                CatalogProperties.FILE_IO_IMPL, this.implementationClass()
        );
    }

    public final Map<String, String> map(T config) {
        var map = new HashMap<>(mapCommon());
        map.putAll(this.mapSpecific(config));
        Log.debugf("Mapped IO config: %s", map);
        return map;
    }

    public abstract String implementationClass();
}
