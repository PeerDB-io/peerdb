// @generated
impl serde::Serialize for CreateRawTableInput {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.peer_connection_config.is_some() {
            len += 1;
        }
        if !self.flow_job_name.is_empty() {
            len += 1;
        }
        if !self.table_name_mapping.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_flow.CreateRawTableInput", len)?;
        if let Some(v) = self.peer_connection_config.as_ref() {
            struct_ser.serialize_field("peerConnectionConfig", v)?;
        }
        if !self.flow_job_name.is_empty() {
            struct_ser.serialize_field("flowJobName", &self.flow_job_name)?;
        }
        if !self.table_name_mapping.is_empty() {
            struct_ser.serialize_field("tableNameMapping", &self.table_name_mapping)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CreateRawTableInput {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "peer_connection_config",
            "peerConnectionConfig",
            "flow_job_name",
            "flowJobName",
            "table_name_mapping",
            "tableNameMapping",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            PeerConnectionConfig,
            FlowJobName,
            TableNameMapping,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "peerConnectionConfig" | "peer_connection_config" => Ok(GeneratedField::PeerConnectionConfig),
                            "flowJobName" | "flow_job_name" => Ok(GeneratedField::FlowJobName),
                            "tableNameMapping" | "table_name_mapping" => Ok(GeneratedField::TableNameMapping),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CreateRawTableInput;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_flow.CreateRawTableInput")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CreateRawTableInput, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut peer_connection_config__ = None;
                let mut flow_job_name__ = None;
                let mut table_name_mapping__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::PeerConnectionConfig => {
                            if peer_connection_config__.is_some() {
                                return Err(serde::de::Error::duplicate_field("peerConnectionConfig"));
                            }
                            peer_connection_config__ = map.next_value()?;
                        }
                        GeneratedField::FlowJobName => {
                            if flow_job_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("flowJobName"));
                            }
                            flow_job_name__ = Some(map.next_value()?);
                        }
                        GeneratedField::TableNameMapping => {
                            if table_name_mapping__.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableNameMapping"));
                            }
                            table_name_mapping__ = Some(
                                map.next_value::<std::collections::HashMap<_, _>>()?
                            );
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(CreateRawTableInput {
                    peer_connection_config: peer_connection_config__,
                    flow_job_name: flow_job_name__.unwrap_or_default(),
                    table_name_mapping: table_name_mapping__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_flow.CreateRawTableInput", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CreateRawTableOutput {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.table_identifier.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_flow.CreateRawTableOutput", len)?;
        if !self.table_identifier.is_empty() {
            struct_ser.serialize_field("tableIdentifier", &self.table_identifier)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CreateRawTableOutput {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "table_identifier",
            "tableIdentifier",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            TableIdentifier,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "tableIdentifier" | "table_identifier" => Ok(GeneratedField::TableIdentifier),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CreateRawTableOutput;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_flow.CreateRawTableOutput")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CreateRawTableOutput, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut table_identifier__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::TableIdentifier => {
                            if table_identifier__.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableIdentifier"));
                            }
                            table_identifier__ = Some(map.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(CreateRawTableOutput {
                    table_identifier: table_identifier__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_flow.CreateRawTableOutput", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for DropFlowInput {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.flow_name.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_flow.DropFlowInput", len)?;
        if !self.flow_name.is_empty() {
            struct_ser.serialize_field("flowName", &self.flow_name)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for DropFlowInput {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "flow_name",
            "flowName",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            FlowName,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "flowName" | "flow_name" => Ok(GeneratedField::FlowName),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = DropFlowInput;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_flow.DropFlowInput")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<DropFlowInput, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut flow_name__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::FlowName => {
                            if flow_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("flowName"));
                            }
                            flow_name__ = Some(map.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(DropFlowInput {
                    flow_name: flow_name__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_flow.DropFlowInput", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for EnsurePullabilityInput {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.peer_connection_config.is_some() {
            len += 1;
        }
        if !self.flow_job_name.is_empty() {
            len += 1;
        }
        if !self.source_table_identifier.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_flow.EnsurePullabilityInput", len)?;
        if let Some(v) = self.peer_connection_config.as_ref() {
            struct_ser.serialize_field("peerConnectionConfig", v)?;
        }
        if !self.flow_job_name.is_empty() {
            struct_ser.serialize_field("flowJobName", &self.flow_job_name)?;
        }
        if !self.source_table_identifier.is_empty() {
            struct_ser.serialize_field("sourceTableIdentifier", &self.source_table_identifier)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for EnsurePullabilityInput {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "peer_connection_config",
            "peerConnectionConfig",
            "flow_job_name",
            "flowJobName",
            "source_table_identifier",
            "sourceTableIdentifier",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            PeerConnectionConfig,
            FlowJobName,
            SourceTableIdentifier,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "peerConnectionConfig" | "peer_connection_config" => Ok(GeneratedField::PeerConnectionConfig),
                            "flowJobName" | "flow_job_name" => Ok(GeneratedField::FlowJobName),
                            "sourceTableIdentifier" | "source_table_identifier" => Ok(GeneratedField::SourceTableIdentifier),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = EnsurePullabilityInput;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_flow.EnsurePullabilityInput")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<EnsurePullabilityInput, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut peer_connection_config__ = None;
                let mut flow_job_name__ = None;
                let mut source_table_identifier__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::PeerConnectionConfig => {
                            if peer_connection_config__.is_some() {
                                return Err(serde::de::Error::duplicate_field("peerConnectionConfig"));
                            }
                            peer_connection_config__ = map.next_value()?;
                        }
                        GeneratedField::FlowJobName => {
                            if flow_job_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("flowJobName"));
                            }
                            flow_job_name__ = Some(map.next_value()?);
                        }
                        GeneratedField::SourceTableIdentifier => {
                            if source_table_identifier__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sourceTableIdentifier"));
                            }
                            source_table_identifier__ = Some(map.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(EnsurePullabilityInput {
                    peer_connection_config: peer_connection_config__,
                    flow_job_name: flow_job_name__.unwrap_or_default(),
                    source_table_identifier: source_table_identifier__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_flow.EnsurePullabilityInput", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for EnsurePullabilityOutput {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.table_identifier.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_flow.EnsurePullabilityOutput", len)?;
        if let Some(v) = self.table_identifier.as_ref() {
            struct_ser.serialize_field("tableIdentifier", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for EnsurePullabilityOutput {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "table_identifier",
            "tableIdentifier",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            TableIdentifier,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "tableIdentifier" | "table_identifier" => Ok(GeneratedField::TableIdentifier),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = EnsurePullabilityOutput;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_flow.EnsurePullabilityOutput")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<EnsurePullabilityOutput, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut table_identifier__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::TableIdentifier => {
                            if table_identifier__.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableIdentifier"));
                            }
                            table_identifier__ = map.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(EnsurePullabilityOutput {
                    table_identifier: table_identifier__,
                })
            }
        }
        deserializer.deserialize_struct("peerdb_flow.EnsurePullabilityOutput", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for FlowConnectionConfigs {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.source.is_some() {
            len += 1;
        }
        if self.destination.is_some() {
            len += 1;
        }
        if !self.flow_job_name.is_empty() {
            len += 1;
        }
        if self.table_schema.is_some() {
            len += 1;
        }
        if !self.table_name_mapping.is_empty() {
            len += 1;
        }
        if !self.src_table_id_name_mapping.is_empty() {
            len += 1;
        }
        if !self.table_name_schema_mapping.is_empty() {
            len += 1;
        }
        if self.metadata_peer.is_some() {
            len += 1;
        }
        if self.max_batch_size != 0 {
            len += 1;
        }
        if self.do_initial_copy {
            len += 1;
        }
        if !self.publication_name.is_empty() {
            len += 1;
        }
        if self.snapshot_num_rows_per_partition != 0 {
            len += 1;
        }
        if self.snapshot_max_parallel_workers != 0 {
            len += 1;
        }
        if self.snapshot_num_tables_in_parallel != 0 {
            len += 1;
        }
        if self.snapshot_sync_mode != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_flow.FlowConnectionConfigs", len)?;
        if let Some(v) = self.source.as_ref() {
            struct_ser.serialize_field("source", v)?;
        }
        if let Some(v) = self.destination.as_ref() {
            struct_ser.serialize_field("destination", v)?;
        }
        if !self.flow_job_name.is_empty() {
            struct_ser.serialize_field("flowJobName", &self.flow_job_name)?;
        }
        if let Some(v) = self.table_schema.as_ref() {
            struct_ser.serialize_field("tableSchema", v)?;
        }
        if !self.table_name_mapping.is_empty() {
            struct_ser.serialize_field("tableNameMapping", &self.table_name_mapping)?;
        }
        if !self.src_table_id_name_mapping.is_empty() {
            struct_ser.serialize_field("srcTableIdNameMapping", &self.src_table_id_name_mapping)?;
        }
        if !self.table_name_schema_mapping.is_empty() {
            struct_ser.serialize_field("tableNameSchemaMapping", &self.table_name_schema_mapping)?;
        }
        if let Some(v) = self.metadata_peer.as_ref() {
            struct_ser.serialize_field("metadataPeer", v)?;
        }
        if self.max_batch_size != 0 {
            struct_ser.serialize_field("maxBatchSize", &self.max_batch_size)?;
        }
        if self.do_initial_copy {
            struct_ser.serialize_field("doInitialCopy", &self.do_initial_copy)?;
        }
        if !self.publication_name.is_empty() {
            struct_ser.serialize_field("publicationName", &self.publication_name)?;
        }
        if self.snapshot_num_rows_per_partition != 0 {
            struct_ser.serialize_field("snapshotNumRowsPerPartition", &self.snapshot_num_rows_per_partition)?;
        }
        if self.snapshot_max_parallel_workers != 0 {
            struct_ser.serialize_field("snapshotMaxParallelWorkers", &self.snapshot_max_parallel_workers)?;
        }
        if self.snapshot_num_tables_in_parallel != 0 {
            struct_ser.serialize_field("snapshotNumTablesInParallel", &self.snapshot_num_tables_in_parallel)?;
        }
        if self.snapshot_sync_mode != 0 {
            let v = QRepSyncMode::from_i32(self.snapshot_sync_mode)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.snapshot_sync_mode)))?;
            struct_ser.serialize_field("snapshotSyncMode", &v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for FlowConnectionConfigs {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "source",
            "destination",
            "flow_job_name",
            "flowJobName",
            "table_schema",
            "tableSchema",
            "table_name_mapping",
            "tableNameMapping",
            "src_table_id_name_mapping",
            "srcTableIdNameMapping",
            "table_name_schema_mapping",
            "tableNameSchemaMapping",
            "metadata_peer",
            "metadataPeer",
            "max_batch_size",
            "maxBatchSize",
            "do_initial_copy",
            "doInitialCopy",
            "publication_name",
            "publicationName",
            "snapshot_num_rows_per_partition",
            "snapshotNumRowsPerPartition",
            "snapshot_max_parallel_workers",
            "snapshotMaxParallelWorkers",
            "snapshot_num_tables_in_parallel",
            "snapshotNumTablesInParallel",
            "snapshot_sync_mode",
            "snapshotSyncMode",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Source,
            Destination,
            FlowJobName,
            TableSchema,
            TableNameMapping,
            SrcTableIdNameMapping,
            TableNameSchemaMapping,
            MetadataPeer,
            MaxBatchSize,
            DoInitialCopy,
            PublicationName,
            SnapshotNumRowsPerPartition,
            SnapshotMaxParallelWorkers,
            SnapshotNumTablesInParallel,
            SnapshotSyncMode,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "source" => Ok(GeneratedField::Source),
                            "destination" => Ok(GeneratedField::Destination),
                            "flowJobName" | "flow_job_name" => Ok(GeneratedField::FlowJobName),
                            "tableSchema" | "table_schema" => Ok(GeneratedField::TableSchema),
                            "tableNameMapping" | "table_name_mapping" => Ok(GeneratedField::TableNameMapping),
                            "srcTableIdNameMapping" | "src_table_id_name_mapping" => Ok(GeneratedField::SrcTableIdNameMapping),
                            "tableNameSchemaMapping" | "table_name_schema_mapping" => Ok(GeneratedField::TableNameSchemaMapping),
                            "metadataPeer" | "metadata_peer" => Ok(GeneratedField::MetadataPeer),
                            "maxBatchSize" | "max_batch_size" => Ok(GeneratedField::MaxBatchSize),
                            "doInitialCopy" | "do_initial_copy" => Ok(GeneratedField::DoInitialCopy),
                            "publicationName" | "publication_name" => Ok(GeneratedField::PublicationName),
                            "snapshotNumRowsPerPartition" | "snapshot_num_rows_per_partition" => Ok(GeneratedField::SnapshotNumRowsPerPartition),
                            "snapshotMaxParallelWorkers" | "snapshot_max_parallel_workers" => Ok(GeneratedField::SnapshotMaxParallelWorkers),
                            "snapshotNumTablesInParallel" | "snapshot_num_tables_in_parallel" => Ok(GeneratedField::SnapshotNumTablesInParallel),
                            "snapshotSyncMode" | "snapshot_sync_mode" => Ok(GeneratedField::SnapshotSyncMode),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = FlowConnectionConfigs;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_flow.FlowConnectionConfigs")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<FlowConnectionConfigs, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut source__ = None;
                let mut destination__ = None;
                let mut flow_job_name__ = None;
                let mut table_schema__ = None;
                let mut table_name_mapping__ = None;
                let mut src_table_id_name_mapping__ = None;
                let mut table_name_schema_mapping__ = None;
                let mut metadata_peer__ = None;
                let mut max_batch_size__ = None;
                let mut do_initial_copy__ = None;
                let mut publication_name__ = None;
                let mut snapshot_num_rows_per_partition__ = None;
                let mut snapshot_max_parallel_workers__ = None;
                let mut snapshot_num_tables_in_parallel__ = None;
                let mut snapshot_sync_mode__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Source => {
                            if source__.is_some() {
                                return Err(serde::de::Error::duplicate_field("source"));
                            }
                            source__ = map.next_value()?;
                        }
                        GeneratedField::Destination => {
                            if destination__.is_some() {
                                return Err(serde::de::Error::duplicate_field("destination"));
                            }
                            destination__ = map.next_value()?;
                        }
                        GeneratedField::FlowJobName => {
                            if flow_job_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("flowJobName"));
                            }
                            flow_job_name__ = Some(map.next_value()?);
                        }
                        GeneratedField::TableSchema => {
                            if table_schema__.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableSchema"));
                            }
                            table_schema__ = map.next_value()?;
                        }
                        GeneratedField::TableNameMapping => {
                            if table_name_mapping__.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableNameMapping"));
                            }
                            table_name_mapping__ = Some(
                                map.next_value::<std::collections::HashMap<_, _>>()?
                            );
                        }
                        GeneratedField::SrcTableIdNameMapping => {
                            if src_table_id_name_mapping__.is_some() {
                                return Err(serde::de::Error::duplicate_field("srcTableIdNameMapping"));
                            }
                            src_table_id_name_mapping__ = Some(
                                map.next_value::<std::collections::HashMap<::pbjson::private::NumberDeserialize<u32>, _>>()?
                                    .into_iter().map(|(k,v)| (k.0, v)).collect()
                            );
                        }
                        GeneratedField::TableNameSchemaMapping => {
                            if table_name_schema_mapping__.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableNameSchemaMapping"));
                            }
                            table_name_schema_mapping__ = Some(
                                map.next_value::<std::collections::HashMap<_, _>>()?
                            );
                        }
                        GeneratedField::MetadataPeer => {
                            if metadata_peer__.is_some() {
                                return Err(serde::de::Error::duplicate_field("metadataPeer"));
                            }
                            metadata_peer__ = map.next_value()?;
                        }
                        GeneratedField::MaxBatchSize => {
                            if max_batch_size__.is_some() {
                                return Err(serde::de::Error::duplicate_field("maxBatchSize"));
                            }
                            max_batch_size__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::DoInitialCopy => {
                            if do_initial_copy__.is_some() {
                                return Err(serde::de::Error::duplicate_field("doInitialCopy"));
                            }
                            do_initial_copy__ = Some(map.next_value()?);
                        }
                        GeneratedField::PublicationName => {
                            if publication_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("publicationName"));
                            }
                            publication_name__ = Some(map.next_value()?);
                        }
                        GeneratedField::SnapshotNumRowsPerPartition => {
                            if snapshot_num_rows_per_partition__.is_some() {
                                return Err(serde::de::Error::duplicate_field("snapshotNumRowsPerPartition"));
                            }
                            snapshot_num_rows_per_partition__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::SnapshotMaxParallelWorkers => {
                            if snapshot_max_parallel_workers__.is_some() {
                                return Err(serde::de::Error::duplicate_field("snapshotMaxParallelWorkers"));
                            }
                            snapshot_max_parallel_workers__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::SnapshotNumTablesInParallel => {
                            if snapshot_num_tables_in_parallel__.is_some() {
                                return Err(serde::de::Error::duplicate_field("snapshotNumTablesInParallel"));
                            }
                            snapshot_num_tables_in_parallel__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::SnapshotSyncMode => {
                            if snapshot_sync_mode__.is_some() {
                                return Err(serde::de::Error::duplicate_field("snapshotSyncMode"));
                            }
                            snapshot_sync_mode__ = Some(map.next_value::<QRepSyncMode>()? as i32);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(FlowConnectionConfigs {
                    source: source__,
                    destination: destination__,
                    flow_job_name: flow_job_name__.unwrap_or_default(),
                    table_schema: table_schema__,
                    table_name_mapping: table_name_mapping__.unwrap_or_default(),
                    src_table_id_name_mapping: src_table_id_name_mapping__.unwrap_or_default(),
                    table_name_schema_mapping: table_name_schema_mapping__.unwrap_or_default(),
                    metadata_peer: metadata_peer__,
                    max_batch_size: max_batch_size__.unwrap_or_default(),
                    do_initial_copy: do_initial_copy__.unwrap_or_default(),
                    publication_name: publication_name__.unwrap_or_default(),
                    snapshot_num_rows_per_partition: snapshot_num_rows_per_partition__.unwrap_or_default(),
                    snapshot_max_parallel_workers: snapshot_max_parallel_workers__.unwrap_or_default(),
                    snapshot_num_tables_in_parallel: snapshot_num_tables_in_parallel__.unwrap_or_default(),
                    snapshot_sync_mode: snapshot_sync_mode__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_flow.FlowConnectionConfigs", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for GetLastSyncedIdInput {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.peer_connection_config.is_some() {
            len += 1;
        }
        if !self.flow_job_name.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_flow.GetLastSyncedIDInput", len)?;
        if let Some(v) = self.peer_connection_config.as_ref() {
            struct_ser.serialize_field("peerConnectionConfig", v)?;
        }
        if !self.flow_job_name.is_empty() {
            struct_ser.serialize_field("flowJobName", &self.flow_job_name)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for GetLastSyncedIdInput {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "peer_connection_config",
            "peerConnectionConfig",
            "flow_job_name",
            "flowJobName",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            PeerConnectionConfig,
            FlowJobName,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "peerConnectionConfig" | "peer_connection_config" => Ok(GeneratedField::PeerConnectionConfig),
                            "flowJobName" | "flow_job_name" => Ok(GeneratedField::FlowJobName),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = GetLastSyncedIdInput;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_flow.GetLastSyncedIDInput")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<GetLastSyncedIdInput, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut peer_connection_config__ = None;
                let mut flow_job_name__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::PeerConnectionConfig => {
                            if peer_connection_config__.is_some() {
                                return Err(serde::de::Error::duplicate_field("peerConnectionConfig"));
                            }
                            peer_connection_config__ = map.next_value()?;
                        }
                        GeneratedField::FlowJobName => {
                            if flow_job_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("flowJobName"));
                            }
                            flow_job_name__ = Some(map.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(GetLastSyncedIdInput {
                    peer_connection_config: peer_connection_config__,
                    flow_job_name: flow_job_name__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_flow.GetLastSyncedIDInput", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for GetTableSchemaInput {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.peer_connection_config.is_some() {
            len += 1;
        }
        if !self.table_identifier.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_flow.GetTableSchemaInput", len)?;
        if let Some(v) = self.peer_connection_config.as_ref() {
            struct_ser.serialize_field("peerConnectionConfig", v)?;
        }
        if !self.table_identifier.is_empty() {
            struct_ser.serialize_field("tableIdentifier", &self.table_identifier)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for GetTableSchemaInput {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "peer_connection_config",
            "peerConnectionConfig",
            "table_identifier",
            "tableIdentifier",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            PeerConnectionConfig,
            TableIdentifier,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "peerConnectionConfig" | "peer_connection_config" => Ok(GeneratedField::PeerConnectionConfig),
                            "tableIdentifier" | "table_identifier" => Ok(GeneratedField::TableIdentifier),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = GetTableSchemaInput;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_flow.GetTableSchemaInput")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<GetTableSchemaInput, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut peer_connection_config__ = None;
                let mut table_identifier__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::PeerConnectionConfig => {
                            if peer_connection_config__.is_some() {
                                return Err(serde::de::Error::duplicate_field("peerConnectionConfig"));
                            }
                            peer_connection_config__ = map.next_value()?;
                        }
                        GeneratedField::TableIdentifier => {
                            if table_identifier__.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableIdentifier"));
                            }
                            table_identifier__ = Some(map.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(GetTableSchemaInput {
                    peer_connection_config: peer_connection_config__,
                    table_identifier: table_identifier__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_flow.GetTableSchemaInput", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for IntPartitionRange {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.start != 0 {
            len += 1;
        }
        if self.end != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_flow.IntPartitionRange", len)?;
        if self.start != 0 {
            struct_ser.serialize_field("start", ToString::to_string(&self.start).as_str())?;
        }
        if self.end != 0 {
            struct_ser.serialize_field("end", ToString::to_string(&self.end).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for IntPartitionRange {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "start",
            "end",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Start,
            End,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "start" => Ok(GeneratedField::Start),
                            "end" => Ok(GeneratedField::End),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = IntPartitionRange;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_flow.IntPartitionRange")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<IntPartitionRange, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut start__ = None;
                let mut end__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Start => {
                            if start__.is_some() {
                                return Err(serde::de::Error::duplicate_field("start"));
                            }
                            start__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::End => {
                            if end__.is_some() {
                                return Err(serde::de::Error::duplicate_field("end"));
                            }
                            end__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(IntPartitionRange {
                    start: start__.unwrap_or_default(),
                    end: end__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_flow.IntPartitionRange", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for LastSyncState {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.checkpoint != 0 {
            len += 1;
        }
        if self.last_synced_at.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_flow.LastSyncState", len)?;
        if self.checkpoint != 0 {
            struct_ser.serialize_field("checkpoint", ToString::to_string(&self.checkpoint).as_str())?;
        }
        if let Some(v) = self.last_synced_at.as_ref() {
            struct_ser.serialize_field("lastSyncedAt", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for LastSyncState {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "checkpoint",
            "last_synced_at",
            "lastSyncedAt",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Checkpoint,
            LastSyncedAt,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "checkpoint" => Ok(GeneratedField::Checkpoint),
                            "lastSyncedAt" | "last_synced_at" => Ok(GeneratedField::LastSyncedAt),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = LastSyncState;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_flow.LastSyncState")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<LastSyncState, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut checkpoint__ = None;
                let mut last_synced_at__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Checkpoint => {
                            if checkpoint__.is_some() {
                                return Err(serde::de::Error::duplicate_field("checkpoint"));
                            }
                            checkpoint__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::LastSyncedAt => {
                            if last_synced_at__.is_some() {
                                return Err(serde::de::Error::duplicate_field("lastSyncedAt"));
                            }
                            last_synced_at__ = map.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(LastSyncState {
                    checkpoint: checkpoint__.unwrap_or_default(),
                    last_synced_at: last_synced_at__,
                })
            }
        }
        deserializer.deserialize_struct("peerdb_flow.LastSyncState", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for NormalizeFlowOptions {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.batch_size != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_flow.NormalizeFlowOptions", len)?;
        if self.batch_size != 0 {
            struct_ser.serialize_field("batchSize", &self.batch_size)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for NormalizeFlowOptions {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "batch_size",
            "batchSize",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            BatchSize,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "batchSize" | "batch_size" => Ok(GeneratedField::BatchSize),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = NormalizeFlowOptions;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_flow.NormalizeFlowOptions")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<NormalizeFlowOptions, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut batch_size__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::BatchSize => {
                            if batch_size__.is_some() {
                                return Err(serde::de::Error::duplicate_field("batchSize"));
                            }
                            batch_size__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(NormalizeFlowOptions {
                    batch_size: batch_size__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_flow.NormalizeFlowOptions", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PartitionRange {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.range.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_flow.PartitionRange", len)?;
        if let Some(v) = self.range.as_ref() {
            match v {
                partition_range::Range::IntRange(v) => {
                    struct_ser.serialize_field("intRange", v)?;
                }
                partition_range::Range::TimestampRange(v) => {
                    struct_ser.serialize_field("timestampRange", v)?;
                }
                partition_range::Range::TidRange(v) => {
                    struct_ser.serialize_field("tidRange", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PartitionRange {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "int_range",
            "intRange",
            "timestamp_range",
            "timestampRange",
            "tid_range",
            "tidRange",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            IntRange,
            TimestampRange,
            TidRange,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "intRange" | "int_range" => Ok(GeneratedField::IntRange),
                            "timestampRange" | "timestamp_range" => Ok(GeneratedField::TimestampRange),
                            "tidRange" | "tid_range" => Ok(GeneratedField::TidRange),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PartitionRange;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_flow.PartitionRange")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<PartitionRange, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut range__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::IntRange => {
                            if range__.is_some() {
                                return Err(serde::de::Error::duplicate_field("intRange"));
                            }
                            range__ = map.next_value::<::std::option::Option<_>>()?.map(partition_range::Range::IntRange)
;
                        }
                        GeneratedField::TimestampRange => {
                            if range__.is_some() {
                                return Err(serde::de::Error::duplicate_field("timestampRange"));
                            }
                            range__ = map.next_value::<::std::option::Option<_>>()?.map(partition_range::Range::TimestampRange)
;
                        }
                        GeneratedField::TidRange => {
                            if range__.is_some() {
                                return Err(serde::de::Error::duplicate_field("tidRange"));
                            }
                            range__ = map.next_value::<::std::option::Option<_>>()?.map(partition_range::Range::TidRange)
;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(PartitionRange {
                    range: range__,
                })
            }
        }
        deserializer.deserialize_struct("peerdb_flow.PartitionRange", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PostgresTableIdentifier {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.rel_id != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_flow.PostgresTableIdentifier", len)?;
        if self.rel_id != 0 {
            struct_ser.serialize_field("relId", &self.rel_id)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PostgresTableIdentifier {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "rel_id",
            "relId",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            RelId,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "relId" | "rel_id" => Ok(GeneratedField::RelId),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PostgresTableIdentifier;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_flow.PostgresTableIdentifier")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<PostgresTableIdentifier, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut rel_id__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::RelId => {
                            if rel_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("relId"));
                            }
                            rel_id__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(PostgresTableIdentifier {
                    rel_id: rel_id__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_flow.PostgresTableIdentifier", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for QRepConfig {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.flow_job_name.is_empty() {
            len += 1;
        }
        if self.source_peer.is_some() {
            len += 1;
        }
        if self.destination_peer.is_some() {
            len += 1;
        }
        if !self.destination_table_identifier.is_empty() {
            len += 1;
        }
        if !self.query.is_empty() {
            len += 1;
        }
        if !self.watermark_table.is_empty() {
            len += 1;
        }
        if !self.watermark_column.is_empty() {
            len += 1;
        }
        if self.initial_copy_only {
            len += 1;
        }
        if self.sync_mode != 0 {
            len += 1;
        }
        if self.batch_size_int != 0 {
            len += 1;
        }
        if self.batch_duration_seconds != 0 {
            len += 1;
        }
        if self.max_parallel_workers != 0 {
            len += 1;
        }
        if self.wait_between_batches_seconds != 0 {
            len += 1;
        }
        if self.write_mode.is_some() {
            len += 1;
        }
        if !self.staging_path.is_empty() {
            len += 1;
        }
        if self.num_rows_per_partition != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_flow.QRepConfig", len)?;
        if !self.flow_job_name.is_empty() {
            struct_ser.serialize_field("flowJobName", &self.flow_job_name)?;
        }
        if let Some(v) = self.source_peer.as_ref() {
            struct_ser.serialize_field("sourcePeer", v)?;
        }
        if let Some(v) = self.destination_peer.as_ref() {
            struct_ser.serialize_field("destinationPeer", v)?;
        }
        if !self.destination_table_identifier.is_empty() {
            struct_ser.serialize_field("destinationTableIdentifier", &self.destination_table_identifier)?;
        }
        if !self.query.is_empty() {
            struct_ser.serialize_field("query", &self.query)?;
        }
        if !self.watermark_table.is_empty() {
            struct_ser.serialize_field("watermarkTable", &self.watermark_table)?;
        }
        if !self.watermark_column.is_empty() {
            struct_ser.serialize_field("watermarkColumn", &self.watermark_column)?;
        }
        if self.initial_copy_only {
            struct_ser.serialize_field("initialCopyOnly", &self.initial_copy_only)?;
        }
        if self.sync_mode != 0 {
            let v = QRepSyncMode::from_i32(self.sync_mode)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.sync_mode)))?;
            struct_ser.serialize_field("syncMode", &v)?;
        }
        if self.batch_size_int != 0 {
            struct_ser.serialize_field("batchSizeInt", &self.batch_size_int)?;
        }
        if self.batch_duration_seconds != 0 {
            struct_ser.serialize_field("batchDurationSeconds", &self.batch_duration_seconds)?;
        }
        if self.max_parallel_workers != 0 {
            struct_ser.serialize_field("maxParallelWorkers", &self.max_parallel_workers)?;
        }
        if self.wait_between_batches_seconds != 0 {
            struct_ser.serialize_field("waitBetweenBatchesSeconds", &self.wait_between_batches_seconds)?;
        }
        if let Some(v) = self.write_mode.as_ref() {
            struct_ser.serialize_field("writeMode", v)?;
        }
        if !self.staging_path.is_empty() {
            struct_ser.serialize_field("stagingPath", &self.staging_path)?;
        }
        if self.num_rows_per_partition != 0 {
            struct_ser.serialize_field("numRowsPerPartition", &self.num_rows_per_partition)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for QRepConfig {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "flow_job_name",
            "flowJobName",
            "source_peer",
            "sourcePeer",
            "destination_peer",
            "destinationPeer",
            "destination_table_identifier",
            "destinationTableIdentifier",
            "query",
            "watermark_table",
            "watermarkTable",
            "watermark_column",
            "watermarkColumn",
            "initial_copy_only",
            "initialCopyOnly",
            "sync_mode",
            "syncMode",
            "batch_size_int",
            "batchSizeInt",
            "batch_duration_seconds",
            "batchDurationSeconds",
            "max_parallel_workers",
            "maxParallelWorkers",
            "wait_between_batches_seconds",
            "waitBetweenBatchesSeconds",
            "write_mode",
            "writeMode",
            "staging_path",
            "stagingPath",
            "num_rows_per_partition",
            "numRowsPerPartition",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            FlowJobName,
            SourcePeer,
            DestinationPeer,
            DestinationTableIdentifier,
            Query,
            WatermarkTable,
            WatermarkColumn,
            InitialCopyOnly,
            SyncMode,
            BatchSizeInt,
            BatchDurationSeconds,
            MaxParallelWorkers,
            WaitBetweenBatchesSeconds,
            WriteMode,
            StagingPath,
            NumRowsPerPartition,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "flowJobName" | "flow_job_name" => Ok(GeneratedField::FlowJobName),
                            "sourcePeer" | "source_peer" => Ok(GeneratedField::SourcePeer),
                            "destinationPeer" | "destination_peer" => Ok(GeneratedField::DestinationPeer),
                            "destinationTableIdentifier" | "destination_table_identifier" => Ok(GeneratedField::DestinationTableIdentifier),
                            "query" => Ok(GeneratedField::Query),
                            "watermarkTable" | "watermark_table" => Ok(GeneratedField::WatermarkTable),
                            "watermarkColumn" | "watermark_column" => Ok(GeneratedField::WatermarkColumn),
                            "initialCopyOnly" | "initial_copy_only" => Ok(GeneratedField::InitialCopyOnly),
                            "syncMode" | "sync_mode" => Ok(GeneratedField::SyncMode),
                            "batchSizeInt" | "batch_size_int" => Ok(GeneratedField::BatchSizeInt),
                            "batchDurationSeconds" | "batch_duration_seconds" => Ok(GeneratedField::BatchDurationSeconds),
                            "maxParallelWorkers" | "max_parallel_workers" => Ok(GeneratedField::MaxParallelWorkers),
                            "waitBetweenBatchesSeconds" | "wait_between_batches_seconds" => Ok(GeneratedField::WaitBetweenBatchesSeconds),
                            "writeMode" | "write_mode" => Ok(GeneratedField::WriteMode),
                            "stagingPath" | "staging_path" => Ok(GeneratedField::StagingPath),
                            "numRowsPerPartition" | "num_rows_per_partition" => Ok(GeneratedField::NumRowsPerPartition),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = QRepConfig;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_flow.QRepConfig")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<QRepConfig, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut flow_job_name__ = None;
                let mut source_peer__ = None;
                let mut destination_peer__ = None;
                let mut destination_table_identifier__ = None;
                let mut query__ = None;
                let mut watermark_table__ = None;
                let mut watermark_column__ = None;
                let mut initial_copy_only__ = None;
                let mut sync_mode__ = None;
                let mut batch_size_int__ = None;
                let mut batch_duration_seconds__ = None;
                let mut max_parallel_workers__ = None;
                let mut wait_between_batches_seconds__ = None;
                let mut write_mode__ = None;
                let mut staging_path__ = None;
                let mut num_rows_per_partition__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::FlowJobName => {
                            if flow_job_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("flowJobName"));
                            }
                            flow_job_name__ = Some(map.next_value()?);
                        }
                        GeneratedField::SourcePeer => {
                            if source_peer__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sourcePeer"));
                            }
                            source_peer__ = map.next_value()?;
                        }
                        GeneratedField::DestinationPeer => {
                            if destination_peer__.is_some() {
                                return Err(serde::de::Error::duplicate_field("destinationPeer"));
                            }
                            destination_peer__ = map.next_value()?;
                        }
                        GeneratedField::DestinationTableIdentifier => {
                            if destination_table_identifier__.is_some() {
                                return Err(serde::de::Error::duplicate_field("destinationTableIdentifier"));
                            }
                            destination_table_identifier__ = Some(map.next_value()?);
                        }
                        GeneratedField::Query => {
                            if query__.is_some() {
                                return Err(serde::de::Error::duplicate_field("query"));
                            }
                            query__ = Some(map.next_value()?);
                        }
                        GeneratedField::WatermarkTable => {
                            if watermark_table__.is_some() {
                                return Err(serde::de::Error::duplicate_field("watermarkTable"));
                            }
                            watermark_table__ = Some(map.next_value()?);
                        }
                        GeneratedField::WatermarkColumn => {
                            if watermark_column__.is_some() {
                                return Err(serde::de::Error::duplicate_field("watermarkColumn"));
                            }
                            watermark_column__ = Some(map.next_value()?);
                        }
                        GeneratedField::InitialCopyOnly => {
                            if initial_copy_only__.is_some() {
                                return Err(serde::de::Error::duplicate_field("initialCopyOnly"));
                            }
                            initial_copy_only__ = Some(map.next_value()?);
                        }
                        GeneratedField::SyncMode => {
                            if sync_mode__.is_some() {
                                return Err(serde::de::Error::duplicate_field("syncMode"));
                            }
                            sync_mode__ = Some(map.next_value::<QRepSyncMode>()? as i32);
                        }
                        GeneratedField::BatchSizeInt => {
                            if batch_size_int__.is_some() {
                                return Err(serde::de::Error::duplicate_field("batchSizeInt"));
                            }
                            batch_size_int__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::BatchDurationSeconds => {
                            if batch_duration_seconds__.is_some() {
                                return Err(serde::de::Error::duplicate_field("batchDurationSeconds"));
                            }
                            batch_duration_seconds__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::MaxParallelWorkers => {
                            if max_parallel_workers__.is_some() {
                                return Err(serde::de::Error::duplicate_field("maxParallelWorkers"));
                            }
                            max_parallel_workers__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::WaitBetweenBatchesSeconds => {
                            if wait_between_batches_seconds__.is_some() {
                                return Err(serde::de::Error::duplicate_field("waitBetweenBatchesSeconds"));
                            }
                            wait_between_batches_seconds__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::WriteMode => {
                            if write_mode__.is_some() {
                                return Err(serde::de::Error::duplicate_field("writeMode"));
                            }
                            write_mode__ = map.next_value()?;
                        }
                        GeneratedField::StagingPath => {
                            if staging_path__.is_some() {
                                return Err(serde::de::Error::duplicate_field("stagingPath"));
                            }
                            staging_path__ = Some(map.next_value()?);
                        }
                        GeneratedField::NumRowsPerPartition => {
                            if num_rows_per_partition__.is_some() {
                                return Err(serde::de::Error::duplicate_field("numRowsPerPartition"));
                            }
                            num_rows_per_partition__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(QRepConfig {
                    flow_job_name: flow_job_name__.unwrap_or_default(),
                    source_peer: source_peer__,
                    destination_peer: destination_peer__,
                    destination_table_identifier: destination_table_identifier__.unwrap_or_default(),
                    query: query__.unwrap_or_default(),
                    watermark_table: watermark_table__.unwrap_or_default(),
                    watermark_column: watermark_column__.unwrap_or_default(),
                    initial_copy_only: initial_copy_only__.unwrap_or_default(),
                    sync_mode: sync_mode__.unwrap_or_default(),
                    batch_size_int: batch_size_int__.unwrap_or_default(),
                    batch_duration_seconds: batch_duration_seconds__.unwrap_or_default(),
                    max_parallel_workers: max_parallel_workers__.unwrap_or_default(),
                    wait_between_batches_seconds: wait_between_batches_seconds__.unwrap_or_default(),
                    write_mode: write_mode__,
                    staging_path: staging_path__.unwrap_or_default(),
                    num_rows_per_partition: num_rows_per_partition__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_flow.QRepConfig", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for QRepParitionResult {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.partitions.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_flow.QRepParitionResult", len)?;
        if !self.partitions.is_empty() {
            struct_ser.serialize_field("partitions", &self.partitions)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for QRepParitionResult {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "partitions",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Partitions,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "partitions" => Ok(GeneratedField::Partitions),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = QRepParitionResult;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_flow.QRepParitionResult")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<QRepParitionResult, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut partitions__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Partitions => {
                            if partitions__.is_some() {
                                return Err(serde::de::Error::duplicate_field("partitions"));
                            }
                            partitions__ = Some(map.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(QRepParitionResult {
                    partitions: partitions__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_flow.QRepParitionResult", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for QRepPartition {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.partition_id.is_empty() {
            len += 1;
        }
        if self.range.is_some() {
            len += 1;
        }
        if self.full_table_partition {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_flow.QRepPartition", len)?;
        if !self.partition_id.is_empty() {
            struct_ser.serialize_field("partitionId", &self.partition_id)?;
        }
        if let Some(v) = self.range.as_ref() {
            struct_ser.serialize_field("range", v)?;
        }
        if self.full_table_partition {
            struct_ser.serialize_field("fullTablePartition", &self.full_table_partition)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for QRepPartition {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "partition_id",
            "partitionId",
            "range",
            "full_table_partition",
            "fullTablePartition",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            PartitionId,
            Range,
            FullTablePartition,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "partitionId" | "partition_id" => Ok(GeneratedField::PartitionId),
                            "range" => Ok(GeneratedField::Range),
                            "fullTablePartition" | "full_table_partition" => Ok(GeneratedField::FullTablePartition),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = QRepPartition;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_flow.QRepPartition")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<QRepPartition, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut partition_id__ = None;
                let mut range__ = None;
                let mut full_table_partition__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::PartitionId => {
                            if partition_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("partitionId"));
                            }
                            partition_id__ = Some(map.next_value()?);
                        }
                        GeneratedField::Range => {
                            if range__.is_some() {
                                return Err(serde::de::Error::duplicate_field("range"));
                            }
                            range__ = map.next_value()?;
                        }
                        GeneratedField::FullTablePartition => {
                            if full_table_partition__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fullTablePartition"));
                            }
                            full_table_partition__ = Some(map.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(QRepPartition {
                    partition_id: partition_id__.unwrap_or_default(),
                    range: range__,
                    full_table_partition: full_table_partition__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_flow.QRepPartition", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for QRepSyncMode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::QrepSyncModeMultiInsert => "QREP_SYNC_MODE_MULTI_INSERT",
            Self::QrepSyncModeStorageAvro => "QREP_SYNC_MODE_STORAGE_AVRO",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for QRepSyncMode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "QREP_SYNC_MODE_MULTI_INSERT",
            "QREP_SYNC_MODE_STORAGE_AVRO",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = QRepSyncMode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(QRepSyncMode::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(QRepSyncMode::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "QREP_SYNC_MODE_MULTI_INSERT" => Ok(QRepSyncMode::QrepSyncModeMultiInsert),
                    "QREP_SYNC_MODE_STORAGE_AVRO" => Ok(QRepSyncMode::QrepSyncModeStorageAvro),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for QRepWriteMode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.write_type != 0 {
            len += 1;
        }
        if !self.upsert_key_columns.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_flow.QRepWriteMode", len)?;
        if self.write_type != 0 {
            let v = QRepWriteType::from_i32(self.write_type)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.write_type)))?;
            struct_ser.serialize_field("writeType", &v)?;
        }
        if !self.upsert_key_columns.is_empty() {
            struct_ser.serialize_field("upsertKeyColumns", &self.upsert_key_columns)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for QRepWriteMode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "write_type",
            "writeType",
            "upsert_key_columns",
            "upsertKeyColumns",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            WriteType,
            UpsertKeyColumns,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "writeType" | "write_type" => Ok(GeneratedField::WriteType),
                            "upsertKeyColumns" | "upsert_key_columns" => Ok(GeneratedField::UpsertKeyColumns),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = QRepWriteMode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_flow.QRepWriteMode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<QRepWriteMode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut write_type__ = None;
                let mut upsert_key_columns__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::WriteType => {
                            if write_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("writeType"));
                            }
                            write_type__ = Some(map.next_value::<QRepWriteType>()? as i32);
                        }
                        GeneratedField::UpsertKeyColumns => {
                            if upsert_key_columns__.is_some() {
                                return Err(serde::de::Error::duplicate_field("upsertKeyColumns"));
                            }
                            upsert_key_columns__ = Some(map.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(QRepWriteMode {
                    write_type: write_type__.unwrap_or_default(),
                    upsert_key_columns: upsert_key_columns__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_flow.QRepWriteMode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for QRepWriteType {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::QrepWriteModeAppend => "QREP_WRITE_MODE_APPEND",
            Self::QrepWriteModeUpsert => "QREP_WRITE_MODE_UPSERT",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for QRepWriteType {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "QREP_WRITE_MODE_APPEND",
            "QREP_WRITE_MODE_UPSERT",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = QRepWriteType;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(QRepWriteType::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(QRepWriteType::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "QREP_WRITE_MODE_APPEND" => Ok(QRepWriteType::QrepWriteModeAppend),
                    "QREP_WRITE_MODE_UPSERT" => Ok(QRepWriteType::QrepWriteModeUpsert),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for SetupNormalizedTableInput {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.peer_connection_config.is_some() {
            len += 1;
        }
        if !self.table_identifier.is_empty() {
            len += 1;
        }
        if self.source_table_schema.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_flow.SetupNormalizedTableInput", len)?;
        if let Some(v) = self.peer_connection_config.as_ref() {
            struct_ser.serialize_field("peerConnectionConfig", v)?;
        }
        if !self.table_identifier.is_empty() {
            struct_ser.serialize_field("tableIdentifier", &self.table_identifier)?;
        }
        if let Some(v) = self.source_table_schema.as_ref() {
            struct_ser.serialize_field("sourceTableSchema", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SetupNormalizedTableInput {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "peer_connection_config",
            "peerConnectionConfig",
            "table_identifier",
            "tableIdentifier",
            "source_table_schema",
            "sourceTableSchema",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            PeerConnectionConfig,
            TableIdentifier,
            SourceTableSchema,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "peerConnectionConfig" | "peer_connection_config" => Ok(GeneratedField::PeerConnectionConfig),
                            "tableIdentifier" | "table_identifier" => Ok(GeneratedField::TableIdentifier),
                            "sourceTableSchema" | "source_table_schema" => Ok(GeneratedField::SourceTableSchema),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SetupNormalizedTableInput;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_flow.SetupNormalizedTableInput")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<SetupNormalizedTableInput, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut peer_connection_config__ = None;
                let mut table_identifier__ = None;
                let mut source_table_schema__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::PeerConnectionConfig => {
                            if peer_connection_config__.is_some() {
                                return Err(serde::de::Error::duplicate_field("peerConnectionConfig"));
                            }
                            peer_connection_config__ = map.next_value()?;
                        }
                        GeneratedField::TableIdentifier => {
                            if table_identifier__.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableIdentifier"));
                            }
                            table_identifier__ = Some(map.next_value()?);
                        }
                        GeneratedField::SourceTableSchema => {
                            if source_table_schema__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sourceTableSchema"));
                            }
                            source_table_schema__ = map.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(SetupNormalizedTableInput {
                    peer_connection_config: peer_connection_config__,
                    table_identifier: table_identifier__.unwrap_or_default(),
                    source_table_schema: source_table_schema__,
                })
            }
        }
        deserializer.deserialize_struct("peerdb_flow.SetupNormalizedTableInput", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SetupNormalizedTableOutput {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.table_identifier.is_empty() {
            len += 1;
        }
        if self.already_exists {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_flow.SetupNormalizedTableOutput", len)?;
        if !self.table_identifier.is_empty() {
            struct_ser.serialize_field("tableIdentifier", &self.table_identifier)?;
        }
        if self.already_exists {
            struct_ser.serialize_field("alreadyExists", &self.already_exists)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SetupNormalizedTableOutput {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "table_identifier",
            "tableIdentifier",
            "already_exists",
            "alreadyExists",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            TableIdentifier,
            AlreadyExists,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "tableIdentifier" | "table_identifier" => Ok(GeneratedField::TableIdentifier),
                            "alreadyExists" | "already_exists" => Ok(GeneratedField::AlreadyExists),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SetupNormalizedTableOutput;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_flow.SetupNormalizedTableOutput")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<SetupNormalizedTableOutput, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut table_identifier__ = None;
                let mut already_exists__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::TableIdentifier => {
                            if table_identifier__.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableIdentifier"));
                            }
                            table_identifier__ = Some(map.next_value()?);
                        }
                        GeneratedField::AlreadyExists => {
                            if already_exists__.is_some() {
                                return Err(serde::de::Error::duplicate_field("alreadyExists"));
                            }
                            already_exists__ = Some(map.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(SetupNormalizedTableOutput {
                    table_identifier: table_identifier__.unwrap_or_default(),
                    already_exists: already_exists__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_flow.SetupNormalizedTableOutput", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SetupReplicationInput {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.peer_connection_config.is_some() {
            len += 1;
        }
        if !self.flow_job_name.is_empty() {
            len += 1;
        }
        if !self.table_name_mapping.is_empty() {
            len += 1;
        }
        if self.destination_peer.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_flow.SetupReplicationInput", len)?;
        if let Some(v) = self.peer_connection_config.as_ref() {
            struct_ser.serialize_field("peerConnectionConfig", v)?;
        }
        if !self.flow_job_name.is_empty() {
            struct_ser.serialize_field("flowJobName", &self.flow_job_name)?;
        }
        if !self.table_name_mapping.is_empty() {
            struct_ser.serialize_field("tableNameMapping", &self.table_name_mapping)?;
        }
        if let Some(v) = self.destination_peer.as_ref() {
            struct_ser.serialize_field("destinationPeer", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SetupReplicationInput {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "peer_connection_config",
            "peerConnectionConfig",
            "flow_job_name",
            "flowJobName",
            "table_name_mapping",
            "tableNameMapping",
            "destination_peer",
            "destinationPeer",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            PeerConnectionConfig,
            FlowJobName,
            TableNameMapping,
            DestinationPeer,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "peerConnectionConfig" | "peer_connection_config" => Ok(GeneratedField::PeerConnectionConfig),
                            "flowJobName" | "flow_job_name" => Ok(GeneratedField::FlowJobName),
                            "tableNameMapping" | "table_name_mapping" => Ok(GeneratedField::TableNameMapping),
                            "destinationPeer" | "destination_peer" => Ok(GeneratedField::DestinationPeer),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SetupReplicationInput;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_flow.SetupReplicationInput")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<SetupReplicationInput, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut peer_connection_config__ = None;
                let mut flow_job_name__ = None;
                let mut table_name_mapping__ = None;
                let mut destination_peer__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::PeerConnectionConfig => {
                            if peer_connection_config__.is_some() {
                                return Err(serde::de::Error::duplicate_field("peerConnectionConfig"));
                            }
                            peer_connection_config__ = map.next_value()?;
                        }
                        GeneratedField::FlowJobName => {
                            if flow_job_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("flowJobName"));
                            }
                            flow_job_name__ = Some(map.next_value()?);
                        }
                        GeneratedField::TableNameMapping => {
                            if table_name_mapping__.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableNameMapping"));
                            }
                            table_name_mapping__ = Some(
                                map.next_value::<std::collections::HashMap<_, _>>()?
                            );
                        }
                        GeneratedField::DestinationPeer => {
                            if destination_peer__.is_some() {
                                return Err(serde::de::Error::duplicate_field("destinationPeer"));
                            }
                            destination_peer__ = map.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(SetupReplicationInput {
                    peer_connection_config: peer_connection_config__,
                    flow_job_name: flow_job_name__.unwrap_or_default(),
                    table_name_mapping: table_name_mapping__.unwrap_or_default(),
                    destination_peer: destination_peer__,
                })
            }
        }
        deserializer.deserialize_struct("peerdb_flow.SetupReplicationInput", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SetupReplicationOutput {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.slot_name.is_empty() {
            len += 1;
        }
        if !self.snapshot_name.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_flow.SetupReplicationOutput", len)?;
        if !self.slot_name.is_empty() {
            struct_ser.serialize_field("slotName", &self.slot_name)?;
        }
        if !self.snapshot_name.is_empty() {
            struct_ser.serialize_field("snapshotName", &self.snapshot_name)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SetupReplicationOutput {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "slot_name",
            "slotName",
            "snapshot_name",
            "snapshotName",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            SlotName,
            SnapshotName,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "slotName" | "slot_name" => Ok(GeneratedField::SlotName),
                            "snapshotName" | "snapshot_name" => Ok(GeneratedField::SnapshotName),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SetupReplicationOutput;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_flow.SetupReplicationOutput")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<SetupReplicationOutput, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut slot_name__ = None;
                let mut snapshot_name__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::SlotName => {
                            if slot_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("slotName"));
                            }
                            slot_name__ = Some(map.next_value()?);
                        }
                        GeneratedField::SnapshotName => {
                            if snapshot_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("snapshotName"));
                            }
                            snapshot_name__ = Some(map.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(SetupReplicationOutput {
                    slot_name: slot_name__.unwrap_or_default(),
                    snapshot_name: snapshot_name__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_flow.SetupReplicationOutput", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for StartFlowInput {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.last_sync_state.is_some() {
            len += 1;
        }
        if self.flow_connection_configs.is_some() {
            len += 1;
        }
        if self.sync_flow_options.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_flow.StartFlowInput", len)?;
        if let Some(v) = self.last_sync_state.as_ref() {
            struct_ser.serialize_field("lastSyncState", v)?;
        }
        if let Some(v) = self.flow_connection_configs.as_ref() {
            struct_ser.serialize_field("flowConnectionConfigs", v)?;
        }
        if let Some(v) = self.sync_flow_options.as_ref() {
            struct_ser.serialize_field("syncFlowOptions", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for StartFlowInput {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "last_sync_state",
            "lastSyncState",
            "flow_connection_configs",
            "flowConnectionConfigs",
            "sync_flow_options",
            "syncFlowOptions",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            LastSyncState,
            FlowConnectionConfigs,
            SyncFlowOptions,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "lastSyncState" | "last_sync_state" => Ok(GeneratedField::LastSyncState),
                            "flowConnectionConfigs" | "flow_connection_configs" => Ok(GeneratedField::FlowConnectionConfigs),
                            "syncFlowOptions" | "sync_flow_options" => Ok(GeneratedField::SyncFlowOptions),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = StartFlowInput;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_flow.StartFlowInput")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<StartFlowInput, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut last_sync_state__ = None;
                let mut flow_connection_configs__ = None;
                let mut sync_flow_options__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::LastSyncState => {
                            if last_sync_state__.is_some() {
                                return Err(serde::de::Error::duplicate_field("lastSyncState"));
                            }
                            last_sync_state__ = map.next_value()?;
                        }
                        GeneratedField::FlowConnectionConfigs => {
                            if flow_connection_configs__.is_some() {
                                return Err(serde::de::Error::duplicate_field("flowConnectionConfigs"));
                            }
                            flow_connection_configs__ = map.next_value()?;
                        }
                        GeneratedField::SyncFlowOptions => {
                            if sync_flow_options__.is_some() {
                                return Err(serde::de::Error::duplicate_field("syncFlowOptions"));
                            }
                            sync_flow_options__ = map.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(StartFlowInput {
                    last_sync_state: last_sync_state__,
                    flow_connection_configs: flow_connection_configs__,
                    sync_flow_options: sync_flow_options__,
                })
            }
        }
        deserializer.deserialize_struct("peerdb_flow.StartFlowInput", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for StartNormalizeInput {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.flow_connection_configs.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_flow.StartNormalizeInput", len)?;
        if let Some(v) = self.flow_connection_configs.as_ref() {
            struct_ser.serialize_field("flowConnectionConfigs", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for StartNormalizeInput {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "flow_connection_configs",
            "flowConnectionConfigs",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            FlowConnectionConfigs,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "flowConnectionConfigs" | "flow_connection_configs" => Ok(GeneratedField::FlowConnectionConfigs),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = StartNormalizeInput;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_flow.StartNormalizeInput")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<StartNormalizeInput, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut flow_connection_configs__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::FlowConnectionConfigs => {
                            if flow_connection_configs__.is_some() {
                                return Err(serde::de::Error::duplicate_field("flowConnectionConfigs"));
                            }
                            flow_connection_configs__ = map.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(StartNormalizeInput {
                    flow_connection_configs: flow_connection_configs__,
                })
            }
        }
        deserializer.deserialize_struct("peerdb_flow.StartNormalizeInput", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SyncFlowOptions {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.batch_size != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_flow.SyncFlowOptions", len)?;
        if self.batch_size != 0 {
            struct_ser.serialize_field("batchSize", &self.batch_size)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SyncFlowOptions {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "batch_size",
            "batchSize",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            BatchSize,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "batchSize" | "batch_size" => Ok(GeneratedField::BatchSize),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SyncFlowOptions;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_flow.SyncFlowOptions")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<SyncFlowOptions, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut batch_size__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::BatchSize => {
                            if batch_size__.is_some() {
                                return Err(serde::de::Error::duplicate_field("batchSize"));
                            }
                            batch_size__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(SyncFlowOptions {
                    batch_size: batch_size__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_flow.SyncFlowOptions", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Tid {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.block_number != 0 {
            len += 1;
        }
        if self.offset_number != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_flow.TID", len)?;
        if self.block_number != 0 {
            struct_ser.serialize_field("blockNumber", &self.block_number)?;
        }
        if self.offset_number != 0 {
            struct_ser.serialize_field("offsetNumber", &self.offset_number)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Tid {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "block_number",
            "blockNumber",
            "offset_number",
            "offsetNumber",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            BlockNumber,
            OffsetNumber,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "blockNumber" | "block_number" => Ok(GeneratedField::BlockNumber),
                            "offsetNumber" | "offset_number" => Ok(GeneratedField::OffsetNumber),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Tid;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_flow.TID")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<Tid, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut block_number__ = None;
                let mut offset_number__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::BlockNumber => {
                            if block_number__.is_some() {
                                return Err(serde::de::Error::duplicate_field("blockNumber"));
                            }
                            block_number__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::OffsetNumber => {
                            if offset_number__.is_some() {
                                return Err(serde::de::Error::duplicate_field("offsetNumber"));
                            }
                            offset_number__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(Tid {
                    block_number: block_number__.unwrap_or_default(),
                    offset_number: offset_number__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_flow.TID", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for TidPartitionRange {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.start.is_some() {
            len += 1;
        }
        if self.end.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_flow.TIDPartitionRange", len)?;
        if let Some(v) = self.start.as_ref() {
            struct_ser.serialize_field("start", v)?;
        }
        if let Some(v) = self.end.as_ref() {
            struct_ser.serialize_field("end", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for TidPartitionRange {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "start",
            "end",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Start,
            End,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "start" => Ok(GeneratedField::Start),
                            "end" => Ok(GeneratedField::End),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = TidPartitionRange;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_flow.TIDPartitionRange")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<TidPartitionRange, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut start__ = None;
                let mut end__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Start => {
                            if start__.is_some() {
                                return Err(serde::de::Error::duplicate_field("start"));
                            }
                            start__ = map.next_value()?;
                        }
                        GeneratedField::End => {
                            if end__.is_some() {
                                return Err(serde::de::Error::duplicate_field("end"));
                            }
                            end__ = map.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(TidPartitionRange {
                    start: start__,
                    end: end__,
                })
            }
        }
        deserializer.deserialize_struct("peerdb_flow.TIDPartitionRange", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for TableIdentifier {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.table_identifier.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_flow.TableIdentifier", len)?;
        if let Some(v) = self.table_identifier.as_ref() {
            match v {
                table_identifier::TableIdentifier::PostgresTableIdentifier(v) => {
                    struct_ser.serialize_field("postgresTableIdentifier", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for TableIdentifier {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "postgres_table_identifier",
            "postgresTableIdentifier",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            PostgresTableIdentifier,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "postgresTableIdentifier" | "postgres_table_identifier" => Ok(GeneratedField::PostgresTableIdentifier),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = TableIdentifier;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_flow.TableIdentifier")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<TableIdentifier, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut table_identifier__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::PostgresTableIdentifier => {
                            if table_identifier__.is_some() {
                                return Err(serde::de::Error::duplicate_field("postgresTableIdentifier"));
                            }
                            table_identifier__ = map.next_value::<::std::option::Option<_>>()?.map(table_identifier::TableIdentifier::PostgresTableIdentifier)
;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(TableIdentifier {
                    table_identifier: table_identifier__,
                })
            }
        }
        deserializer.deserialize_struct("peerdb_flow.TableIdentifier", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for TableNameMapping {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.source_table_name.is_empty() {
            len += 1;
        }
        if !self.destination_table_name.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_flow.TableNameMapping", len)?;
        if !self.source_table_name.is_empty() {
            struct_ser.serialize_field("sourceTableName", &self.source_table_name)?;
        }
        if !self.destination_table_name.is_empty() {
            struct_ser.serialize_field("destinationTableName", &self.destination_table_name)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for TableNameMapping {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "source_table_name",
            "sourceTableName",
            "destination_table_name",
            "destinationTableName",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            SourceTableName,
            DestinationTableName,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "sourceTableName" | "source_table_name" => Ok(GeneratedField::SourceTableName),
                            "destinationTableName" | "destination_table_name" => Ok(GeneratedField::DestinationTableName),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = TableNameMapping;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_flow.TableNameMapping")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<TableNameMapping, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut source_table_name__ = None;
                let mut destination_table_name__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::SourceTableName => {
                            if source_table_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sourceTableName"));
                            }
                            source_table_name__ = Some(map.next_value()?);
                        }
                        GeneratedField::DestinationTableName => {
                            if destination_table_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("destinationTableName"));
                            }
                            destination_table_name__ = Some(map.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(TableNameMapping {
                    source_table_name: source_table_name__.unwrap_or_default(),
                    destination_table_name: destination_table_name__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_flow.TableNameMapping", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for TableSchema {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.table_identifier.is_empty() {
            len += 1;
        }
        if !self.columns.is_empty() {
            len += 1;
        }
        if !self.primary_key_column.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_flow.TableSchema", len)?;
        if !self.table_identifier.is_empty() {
            struct_ser.serialize_field("tableIdentifier", &self.table_identifier)?;
        }
        if !self.columns.is_empty() {
            struct_ser.serialize_field("columns", &self.columns)?;
        }
        if !self.primary_key_column.is_empty() {
            struct_ser.serialize_field("primaryKeyColumn", &self.primary_key_column)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for TableSchema {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "table_identifier",
            "tableIdentifier",
            "columns",
            "primary_key_column",
            "primaryKeyColumn",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            TableIdentifier,
            Columns,
            PrimaryKeyColumn,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "tableIdentifier" | "table_identifier" => Ok(GeneratedField::TableIdentifier),
                            "columns" => Ok(GeneratedField::Columns),
                            "primaryKeyColumn" | "primary_key_column" => Ok(GeneratedField::PrimaryKeyColumn),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = TableSchema;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_flow.TableSchema")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<TableSchema, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut table_identifier__ = None;
                let mut columns__ = None;
                let mut primary_key_column__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::TableIdentifier => {
                            if table_identifier__.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableIdentifier"));
                            }
                            table_identifier__ = Some(map.next_value()?);
                        }
                        GeneratedField::Columns => {
                            if columns__.is_some() {
                                return Err(serde::de::Error::duplicate_field("columns"));
                            }
                            columns__ = Some(
                                map.next_value::<std::collections::HashMap<_, _>>()?
                            );
                        }
                        GeneratedField::PrimaryKeyColumn => {
                            if primary_key_column__.is_some() {
                                return Err(serde::de::Error::duplicate_field("primaryKeyColumn"));
                            }
                            primary_key_column__ = Some(map.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(TableSchema {
                    table_identifier: table_identifier__.unwrap_or_default(),
                    columns: columns__.unwrap_or_default(),
                    primary_key_column: primary_key_column__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_flow.TableSchema", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for TimestampPartitionRange {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.start.is_some() {
            len += 1;
        }
        if self.end.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_flow.TimestampPartitionRange", len)?;
        if let Some(v) = self.start.as_ref() {
            struct_ser.serialize_field("start", v)?;
        }
        if let Some(v) = self.end.as_ref() {
            struct_ser.serialize_field("end", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for TimestampPartitionRange {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "start",
            "end",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Start,
            End,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "start" => Ok(GeneratedField::Start),
                            "end" => Ok(GeneratedField::End),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = TimestampPartitionRange;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_flow.TimestampPartitionRange")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<TimestampPartitionRange, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut start__ = None;
                let mut end__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Start => {
                            if start__.is_some() {
                                return Err(serde::de::Error::duplicate_field("start"));
                            }
                            start__ = map.next_value()?;
                        }
                        GeneratedField::End => {
                            if end__.is_some() {
                                return Err(serde::de::Error::duplicate_field("end"));
                            }
                            end__ = map.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(TimestampPartitionRange {
                    start: start__,
                    end: end__,
                })
            }
        }
        deserializer.deserialize_struct("peerdb_flow.TimestampPartitionRange", FIELDS, GeneratedVisitor)
    }
}
