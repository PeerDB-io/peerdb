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
        if self.cdc_sync_mode != 0 {
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
        if self.cdc_sync_mode != 0 {
            let v = QRepSyncMode::from_i32(self.cdc_sync_mode)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.cdc_sync_mode)))?;
            struct_ser.serialize_field("cdcSyncMode", &v)?;
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
            "cdc_sync_mode",
            "cdcSyncMode",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            PeerConnectionConfig,
            FlowJobName,
            TableNameMapping,
            CdcSyncMode,
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
                            "cdcSyncMode" | "cdc_sync_mode" => Ok(GeneratedField::CdcSyncMode),
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
                let mut cdc_sync_mode__ = None;
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
                        GeneratedField::CdcSyncMode => {
                            if cdc_sync_mode__.is_some() {
                                return Err(serde::de::Error::duplicate_field("cdcSyncMode"));
                            }
                            cdc_sync_mode__ = Some(map.next_value::<QRepSyncMode>()? as i32);
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
                    cdc_sync_mode: cdc_sync_mode__.unwrap_or_default(),
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
impl serde::Serialize for CreateTablesFromExistingInput {
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
        if self.peer.is_some() {
            len += 1;
        }
        if !self.new_to_existing_table_mapping.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_flow.CreateTablesFromExistingInput", len)?;
        if !self.flow_job_name.is_empty() {
            struct_ser.serialize_field("flowJobName", &self.flow_job_name)?;
        }
        if let Some(v) = self.peer.as_ref() {
            struct_ser.serialize_field("peer", v)?;
        }
        if !self.new_to_existing_table_mapping.is_empty() {
            struct_ser.serialize_field("newToExistingTableMapping", &self.new_to_existing_table_mapping)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CreateTablesFromExistingInput {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "flow_job_name",
            "flowJobName",
            "peer",
            "new_to_existing_table_mapping",
            "newToExistingTableMapping",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            FlowJobName,
            Peer,
            NewToExistingTableMapping,
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
                            "peer" => Ok(GeneratedField::Peer),
                            "newToExistingTableMapping" | "new_to_existing_table_mapping" => Ok(GeneratedField::NewToExistingTableMapping),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CreateTablesFromExistingInput;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_flow.CreateTablesFromExistingInput")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CreateTablesFromExistingInput, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut flow_job_name__ = None;
                let mut peer__ = None;
                let mut new_to_existing_table_mapping__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::FlowJobName => {
                            if flow_job_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("flowJobName"));
                            }
                            flow_job_name__ = Some(map.next_value()?);
                        }
                        GeneratedField::Peer => {
                            if peer__.is_some() {
                                return Err(serde::de::Error::duplicate_field("peer"));
                            }
                            peer__ = map.next_value()?;
                        }
                        GeneratedField::NewToExistingTableMapping => {
                            if new_to_existing_table_mapping__.is_some() {
                                return Err(serde::de::Error::duplicate_field("newToExistingTableMapping"));
                            }
                            new_to_existing_table_mapping__ = Some(
                                map.next_value::<std::collections::HashMap<_, _>>()?
                            );
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(CreateTablesFromExistingInput {
                    flow_job_name: flow_job_name__.unwrap_or_default(),
                    peer: peer__,
                    new_to_existing_table_mapping: new_to_existing_table_mapping__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_flow.CreateTablesFromExistingInput", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CreateTablesFromExistingOutput {
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
        let mut struct_ser = serializer.serialize_struct("peerdb_flow.CreateTablesFromExistingOutput", len)?;
        if !self.flow_job_name.is_empty() {
            struct_ser.serialize_field("flowJobName", &self.flow_job_name)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CreateTablesFromExistingOutput {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "flow_job_name",
            "flowJobName",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
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
            type Value = CreateTablesFromExistingOutput;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_flow.CreateTablesFromExistingOutput")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CreateTablesFromExistingOutput, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut flow_job_name__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
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
                Ok(CreateTablesFromExistingOutput {
                    flow_job_name: flow_job_name__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_flow.CreateTablesFromExistingOutput", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for DeltaAddedColumn {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.column_name.is_empty() {
            len += 1;
        }
        if !self.column_type.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_flow.DeltaAddedColumn", len)?;
        if !self.column_name.is_empty() {
            struct_ser.serialize_field("columnName", &self.column_name)?;
        }
        if !self.column_type.is_empty() {
            struct_ser.serialize_field("columnType", &self.column_type)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for DeltaAddedColumn {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "column_name",
            "columnName",
            "column_type",
            "columnType",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ColumnName,
            ColumnType,
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
                            "columnName" | "column_name" => Ok(GeneratedField::ColumnName),
                            "columnType" | "column_type" => Ok(GeneratedField::ColumnType),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = DeltaAddedColumn;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_flow.DeltaAddedColumn")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<DeltaAddedColumn, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut column_name__ = None;
                let mut column_type__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::ColumnName => {
                            if column_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("columnName"));
                            }
                            column_name__ = Some(map.next_value()?);
                        }
                        GeneratedField::ColumnType => {
                            if column_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("columnType"));
                            }
                            column_type__ = Some(map.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(DeltaAddedColumn {
                    column_name: column_name__.unwrap_or_default(),
                    column_type: column_type__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_flow.DeltaAddedColumn", FIELDS, GeneratedVisitor)
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
impl serde::Serialize for EnsurePullabilityBatchInput {
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
        if !self.source_table_identifiers.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_flow.EnsurePullabilityBatchInput", len)?;
        if let Some(v) = self.peer_connection_config.as_ref() {
            struct_ser.serialize_field("peerConnectionConfig", v)?;
        }
        if !self.flow_job_name.is_empty() {
            struct_ser.serialize_field("flowJobName", &self.flow_job_name)?;
        }
        if !self.source_table_identifiers.is_empty() {
            struct_ser.serialize_field("sourceTableIdentifiers", &self.source_table_identifiers)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for EnsurePullabilityBatchInput {
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
            "source_table_identifiers",
            "sourceTableIdentifiers",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            PeerConnectionConfig,
            FlowJobName,
            SourceTableIdentifiers,
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
                            "sourceTableIdentifiers" | "source_table_identifiers" => Ok(GeneratedField::SourceTableIdentifiers),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = EnsurePullabilityBatchInput;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_flow.EnsurePullabilityBatchInput")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<EnsurePullabilityBatchInput, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut peer_connection_config__ = None;
                let mut flow_job_name__ = None;
                let mut source_table_identifiers__ = None;
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
                        GeneratedField::SourceTableIdentifiers => {
                            if source_table_identifiers__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sourceTableIdentifiers"));
                            }
                            source_table_identifiers__ = Some(map.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(EnsurePullabilityBatchInput {
                    peer_connection_config: peer_connection_config__,
                    flow_job_name: flow_job_name__.unwrap_or_default(),
                    source_table_identifiers: source_table_identifiers__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_flow.EnsurePullabilityBatchInput", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for EnsurePullabilityBatchOutput {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.table_identifier_mapping.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_flow.EnsurePullabilityBatchOutput", len)?;
        if !self.table_identifier_mapping.is_empty() {
            struct_ser.serialize_field("tableIdentifierMapping", &self.table_identifier_mapping)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for EnsurePullabilityBatchOutput {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "table_identifier_mapping",
            "tableIdentifierMapping",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            TableIdentifierMapping,
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
                            "tableIdentifierMapping" | "table_identifier_mapping" => Ok(GeneratedField::TableIdentifierMapping),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = EnsurePullabilityBatchOutput;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_flow.EnsurePullabilityBatchOutput")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<EnsurePullabilityBatchOutput, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut table_identifier_mapping__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::TableIdentifierMapping => {
                            if table_identifier_mapping__.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableIdentifierMapping"));
                            }
                            table_identifier_mapping__ = Some(
                                map.next_value::<std::collections::HashMap<_, _>>()?
                            );
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(EnsurePullabilityBatchOutput {
                    table_identifier_mapping: table_identifier_mapping__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_flow.EnsurePullabilityBatchOutput", FIELDS, GeneratedVisitor)
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
        if !self.table_mappings.is_empty() {
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
        if self.cdc_sync_mode != 0 {
            len += 1;
        }
        if !self.snapshot_staging_path.is_empty() {
            len += 1;
        }
        if !self.cdc_staging_path.is_empty() {
            len += 1;
        }
        if self.soft_delete {
            len += 1;
        }
        if !self.replication_slot_name.is_empty() {
            len += 1;
        }
        if self.push_batch_size != 0 {
            len += 1;
        }
        if self.push_parallelism != 0 {
            len += 1;
        }
        if self.resync {
            len += 1;
        }
        if !self.soft_delete_col_name.is_empty() {
            len += 1;
        }
        if !self.synced_at_col_name.is_empty() {
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
        if !self.table_mappings.is_empty() {
            struct_ser.serialize_field("tableMappings", &self.table_mappings)?;
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
        if self.cdc_sync_mode != 0 {
            let v = QRepSyncMode::from_i32(self.cdc_sync_mode)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.cdc_sync_mode)))?;
            struct_ser.serialize_field("cdcSyncMode", &v)?;
        }
        if !self.snapshot_staging_path.is_empty() {
            struct_ser.serialize_field("snapshotStagingPath", &self.snapshot_staging_path)?;
        }
        if !self.cdc_staging_path.is_empty() {
            struct_ser.serialize_field("cdcStagingPath", &self.cdc_staging_path)?;
        }
        if self.soft_delete {
            struct_ser.serialize_field("softDelete", &self.soft_delete)?;
        }
        if !self.replication_slot_name.is_empty() {
            struct_ser.serialize_field("replicationSlotName", &self.replication_slot_name)?;
        }
        if self.push_batch_size != 0 {
            struct_ser.serialize_field("pushBatchSize", ToString::to_string(&self.push_batch_size).as_str())?;
        }
        if self.push_parallelism != 0 {
            struct_ser.serialize_field("pushParallelism", ToString::to_string(&self.push_parallelism).as_str())?;
        }
        if self.resync {
            struct_ser.serialize_field("resync", &self.resync)?;
        }
        if !self.soft_delete_col_name.is_empty() {
            struct_ser.serialize_field("softDeleteColName", &self.soft_delete_col_name)?;
        }
        if !self.synced_at_col_name.is_empty() {
            struct_ser.serialize_field("syncedAtColName", &self.synced_at_col_name)?;
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
            "table_mappings",
            "tableMappings",
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
            "cdc_sync_mode",
            "cdcSyncMode",
            "snapshot_staging_path",
            "snapshotStagingPath",
            "cdc_staging_path",
            "cdcStagingPath",
            "soft_delete",
            "softDelete",
            "replication_slot_name",
            "replicationSlotName",
            "push_batch_size",
            "pushBatchSize",
            "push_parallelism",
            "pushParallelism",
            "resync",
            "soft_delete_col_name",
            "softDeleteColName",
            "synced_at_col_name",
            "syncedAtColName",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Source,
            Destination,
            FlowJobName,
            TableSchema,
            TableMappings,
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
            CdcSyncMode,
            SnapshotStagingPath,
            CdcStagingPath,
            SoftDelete,
            ReplicationSlotName,
            PushBatchSize,
            PushParallelism,
            Resync,
            SoftDeleteColName,
            SyncedAtColName,
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
                            "tableMappings" | "table_mappings" => Ok(GeneratedField::TableMappings),
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
                            "cdcSyncMode" | "cdc_sync_mode" => Ok(GeneratedField::CdcSyncMode),
                            "snapshotStagingPath" | "snapshot_staging_path" => Ok(GeneratedField::SnapshotStagingPath),
                            "cdcStagingPath" | "cdc_staging_path" => Ok(GeneratedField::CdcStagingPath),
                            "softDelete" | "soft_delete" => Ok(GeneratedField::SoftDelete),
                            "replicationSlotName" | "replication_slot_name" => Ok(GeneratedField::ReplicationSlotName),
                            "pushBatchSize" | "push_batch_size" => Ok(GeneratedField::PushBatchSize),
                            "pushParallelism" | "push_parallelism" => Ok(GeneratedField::PushParallelism),
                            "resync" => Ok(GeneratedField::Resync),
                            "softDeleteColName" | "soft_delete_col_name" => Ok(GeneratedField::SoftDeleteColName),
                            "syncedAtColName" | "synced_at_col_name" => Ok(GeneratedField::SyncedAtColName),
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
                let mut table_mappings__ = None;
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
                let mut cdc_sync_mode__ = None;
                let mut snapshot_staging_path__ = None;
                let mut cdc_staging_path__ = None;
                let mut soft_delete__ = None;
                let mut replication_slot_name__ = None;
                let mut push_batch_size__ = None;
                let mut push_parallelism__ = None;
                let mut resync__ = None;
                let mut soft_delete_col_name__ = None;
                let mut synced_at_col_name__ = None;
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
                        GeneratedField::TableMappings => {
                            if table_mappings__.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableMappings"));
                            }
                            table_mappings__ = Some(map.next_value()?);
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
                        GeneratedField::CdcSyncMode => {
                            if cdc_sync_mode__.is_some() {
                                return Err(serde::de::Error::duplicate_field("cdcSyncMode"));
                            }
                            cdc_sync_mode__ = Some(map.next_value::<QRepSyncMode>()? as i32);
                        }
                        GeneratedField::SnapshotStagingPath => {
                            if snapshot_staging_path__.is_some() {
                                return Err(serde::de::Error::duplicate_field("snapshotStagingPath"));
                            }
                            snapshot_staging_path__ = Some(map.next_value()?);
                        }
                        GeneratedField::CdcStagingPath => {
                            if cdc_staging_path__.is_some() {
                                return Err(serde::de::Error::duplicate_field("cdcStagingPath"));
                            }
                            cdc_staging_path__ = Some(map.next_value()?);
                        }
                        GeneratedField::SoftDelete => {
                            if soft_delete__.is_some() {
                                return Err(serde::de::Error::duplicate_field("softDelete"));
                            }
                            soft_delete__ = Some(map.next_value()?);
                        }
                        GeneratedField::ReplicationSlotName => {
                            if replication_slot_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("replicationSlotName"));
                            }
                            replication_slot_name__ = Some(map.next_value()?);
                        }
                        GeneratedField::PushBatchSize => {
                            if push_batch_size__.is_some() {
                                return Err(serde::de::Error::duplicate_field("pushBatchSize"));
                            }
                            push_batch_size__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::PushParallelism => {
                            if push_parallelism__.is_some() {
                                return Err(serde::de::Error::duplicate_field("pushParallelism"));
                            }
                            push_parallelism__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Resync => {
                            if resync__.is_some() {
                                return Err(serde::de::Error::duplicate_field("resync"));
                            }
                            resync__ = Some(map.next_value()?);
                        }
                        GeneratedField::SoftDeleteColName => {
                            if soft_delete_col_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("softDeleteColName"));
                            }
                            soft_delete_col_name__ = Some(map.next_value()?);
                        }
                        GeneratedField::SyncedAtColName => {
                            if synced_at_col_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("syncedAtColName"));
                            }
                            synced_at_col_name__ = Some(map.next_value()?);
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
                    table_mappings: table_mappings__.unwrap_or_default(),
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
                    cdc_sync_mode: cdc_sync_mode__.unwrap_or_default(),
                    snapshot_staging_path: snapshot_staging_path__.unwrap_or_default(),
                    cdc_staging_path: cdc_staging_path__.unwrap_or_default(),
                    soft_delete: soft_delete__.unwrap_or_default(),
                    replication_slot_name: replication_slot_name__.unwrap_or_default(),
                    push_batch_size: push_batch_size__.unwrap_or_default(),
                    push_parallelism: push_parallelism__.unwrap_or_default(),
                    resync: resync__.unwrap_or_default(),
                    soft_delete_col_name: soft_delete_col_name__.unwrap_or_default(),
                    synced_at_col_name: synced_at_col_name__.unwrap_or_default(),
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
impl serde::Serialize for GetTableSchemaBatchInput {
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
        if !self.table_identifiers.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_flow.GetTableSchemaBatchInput", len)?;
        if let Some(v) = self.peer_connection_config.as_ref() {
            struct_ser.serialize_field("peerConnectionConfig", v)?;
        }
        if !self.table_identifiers.is_empty() {
            struct_ser.serialize_field("tableIdentifiers", &self.table_identifiers)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for GetTableSchemaBatchInput {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "peer_connection_config",
            "peerConnectionConfig",
            "table_identifiers",
            "tableIdentifiers",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            PeerConnectionConfig,
            TableIdentifiers,
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
                            "tableIdentifiers" | "table_identifiers" => Ok(GeneratedField::TableIdentifiers),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = GetTableSchemaBatchInput;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_flow.GetTableSchemaBatchInput")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<GetTableSchemaBatchInput, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut peer_connection_config__ = None;
                let mut table_identifiers__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::PeerConnectionConfig => {
                            if peer_connection_config__.is_some() {
                                return Err(serde::de::Error::duplicate_field("peerConnectionConfig"));
                            }
                            peer_connection_config__ = map.next_value()?;
                        }
                        GeneratedField::TableIdentifiers => {
                            if table_identifiers__.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableIdentifiers"));
                            }
                            table_identifiers__ = Some(map.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(GetTableSchemaBatchInput {
                    peer_connection_config: peer_connection_config__,
                    table_identifiers: table_identifiers__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_flow.GetTableSchemaBatchInput", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for GetTableSchemaBatchOutput {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.table_name_schema_mapping.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_flow.GetTableSchemaBatchOutput", len)?;
        if !self.table_name_schema_mapping.is_empty() {
            struct_ser.serialize_field("tableNameSchemaMapping", &self.table_name_schema_mapping)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for GetTableSchemaBatchOutput {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "table_name_schema_mapping",
            "tableNameSchemaMapping",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            TableNameSchemaMapping,
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
                            "tableNameSchemaMapping" | "table_name_schema_mapping" => Ok(GeneratedField::TableNameSchemaMapping),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = GetTableSchemaBatchOutput;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_flow.GetTableSchemaBatchOutput")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<GetTableSchemaBatchOutput, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut table_name_schema_mapping__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::TableNameSchemaMapping => {
                            if table_name_schema_mapping__.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableNameSchemaMapping"));
                            }
                            table_name_schema_mapping__ = Some(
                                map.next_value::<std::collections::HashMap<_, _>>()?
                            );
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(GetTableSchemaBatchOutput {
                    table_name_schema_mapping: table_name_schema_mapping__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_flow.GetTableSchemaBatchOutput", FIELDS, GeneratedVisitor)
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
        if self.setup_watermark_table_on_destination {
            len += 1;
        }
        if self.dst_table_full_resync {
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
        if self.setup_watermark_table_on_destination {
            struct_ser.serialize_field("setupWatermarkTableOnDestination", &self.setup_watermark_table_on_destination)?;
        }
        if self.dst_table_full_resync {
            struct_ser.serialize_field("dstTableFullResync", &self.dst_table_full_resync)?;
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
            "setup_watermark_table_on_destination",
            "setupWatermarkTableOnDestination",
            "dst_table_full_resync",
            "dstTableFullResync",
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
            SetupWatermarkTableOnDestination,
            DstTableFullResync,
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
                            "setupWatermarkTableOnDestination" | "setup_watermark_table_on_destination" => Ok(GeneratedField::SetupWatermarkTableOnDestination),
                            "dstTableFullResync" | "dst_table_full_resync" => Ok(GeneratedField::DstTableFullResync),
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
                let mut setup_watermark_table_on_destination__ = None;
                let mut dst_table_full_resync__ = None;
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
                        GeneratedField::SetupWatermarkTableOnDestination => {
                            if setup_watermark_table_on_destination__.is_some() {
                                return Err(serde::de::Error::duplicate_field("setupWatermarkTableOnDestination"));
                            }
                            setup_watermark_table_on_destination__ = Some(map.next_value()?);
                        }
                        GeneratedField::DstTableFullResync => {
                            if dst_table_full_resync__.is_some() {
                                return Err(serde::de::Error::duplicate_field("dstTableFullResync"));
                            }
                            dst_table_full_resync__ = Some(map.next_value()?);
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
                    setup_watermark_table_on_destination: setup_watermark_table_on_destination__.unwrap_or_default(),
                    dst_table_full_resync: dst_table_full_resync__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_flow.QRepConfig", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for QRepFlowState {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.last_partition.is_some() {
            len += 1;
        }
        if self.num_partitions_processed != 0 {
            len += 1;
        }
        if self.needs_resync {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_flow.QRepFlowState", len)?;
        if let Some(v) = self.last_partition.as_ref() {
            struct_ser.serialize_field("lastPartition", v)?;
        }
        if self.num_partitions_processed != 0 {
            struct_ser.serialize_field("numPartitionsProcessed", ToString::to_string(&self.num_partitions_processed).as_str())?;
        }
        if self.needs_resync {
            struct_ser.serialize_field("needsResync", &self.needs_resync)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for QRepFlowState {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "last_partition",
            "lastPartition",
            "num_partitions_processed",
            "numPartitionsProcessed",
            "needs_resync",
            "needsResync",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            LastPartition,
            NumPartitionsProcessed,
            NeedsResync,
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
                            "lastPartition" | "last_partition" => Ok(GeneratedField::LastPartition),
                            "numPartitionsProcessed" | "num_partitions_processed" => Ok(GeneratedField::NumPartitionsProcessed),
                            "needsResync" | "needs_resync" => Ok(GeneratedField::NeedsResync),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = QRepFlowState;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_flow.QRepFlowState")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<QRepFlowState, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut last_partition__ = None;
                let mut num_partitions_processed__ = None;
                let mut needs_resync__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::LastPartition => {
                            if last_partition__.is_some() {
                                return Err(serde::de::Error::duplicate_field("lastPartition"));
                            }
                            last_partition__ = map.next_value()?;
                        }
                        GeneratedField::NumPartitionsProcessed => {
                            if num_partitions_processed__.is_some() {
                                return Err(serde::de::Error::duplicate_field("numPartitionsProcessed"));
                            }
                            num_partitions_processed__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::NeedsResync => {
                            if needs_resync__.is_some() {
                                return Err(serde::de::Error::duplicate_field("needsResync"));
                            }
                            needs_resync__ = Some(map.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(QRepFlowState {
                    last_partition: last_partition__,
                    num_partitions_processed: num_partitions_processed__.unwrap_or_default(),
                    needs_resync: needs_resync__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_flow.QRepFlowState", FIELDS, GeneratedVisitor)
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
impl serde::Serialize for QRepPartitionBatch {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.batch_id != 0 {
            len += 1;
        }
        if !self.partitions.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_flow.QRepPartitionBatch", len)?;
        if self.batch_id != 0 {
            struct_ser.serialize_field("batchId", &self.batch_id)?;
        }
        if !self.partitions.is_empty() {
            struct_ser.serialize_field("partitions", &self.partitions)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for QRepPartitionBatch {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "batch_id",
            "batchId",
            "partitions",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            BatchId,
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
                            "batchId" | "batch_id" => Ok(GeneratedField::BatchId),
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
            type Value = QRepPartitionBatch;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_flow.QRepPartitionBatch")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<QRepPartitionBatch, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut batch_id__ = None;
                let mut partitions__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::BatchId => {
                            if batch_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("batchId"));
                            }
                            batch_id__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
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
                Ok(QRepPartitionBatch {
                    batch_id: batch_id__.unwrap_or_default(),
                    partitions: partitions__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_flow.QRepPartitionBatch", FIELDS, GeneratedVisitor)
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
            Self::QrepWriteModeOverwrite => "QREP_WRITE_MODE_OVERWRITE",
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
            "QREP_WRITE_MODE_OVERWRITE",
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
                    "QREP_WRITE_MODE_OVERWRITE" => Ok(QRepWriteType::QrepWriteModeOverwrite),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for RelationMessage {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.relation_id != 0 {
            len += 1;
        }
        if !self.relation_name.is_empty() {
            len += 1;
        }
        if !self.columns.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_flow.RelationMessage", len)?;
        if self.relation_id != 0 {
            struct_ser.serialize_field("relationId", &self.relation_id)?;
        }
        if !self.relation_name.is_empty() {
            struct_ser.serialize_field("relationName", &self.relation_name)?;
        }
        if !self.columns.is_empty() {
            struct_ser.serialize_field("columns", &self.columns)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for RelationMessage {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "relation_id",
            "relationId",
            "relation_name",
            "relationName",
            "columns",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            RelationId,
            RelationName,
            Columns,
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
                            "relationId" | "relation_id" => Ok(GeneratedField::RelationId),
                            "relationName" | "relation_name" => Ok(GeneratedField::RelationName),
                            "columns" => Ok(GeneratedField::Columns),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = RelationMessage;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_flow.RelationMessage")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<RelationMessage, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut relation_id__ = None;
                let mut relation_name__ = None;
                let mut columns__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::RelationId => {
                            if relation_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("relationId"));
                            }
                            relation_id__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::RelationName => {
                            if relation_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("relationName"));
                            }
                            relation_name__ = Some(map.next_value()?);
                        }
                        GeneratedField::Columns => {
                            if columns__.is_some() {
                                return Err(serde::de::Error::duplicate_field("columns"));
                            }
                            columns__ = Some(map.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(RelationMessage {
                    relation_id: relation_id__.unwrap_or_default(),
                    relation_name: relation_name__.unwrap_or_default(),
                    columns: columns__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_flow.RelationMessage", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for RelationMessageColumn {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.flags != 0 {
            len += 1;
        }
        if !self.name.is_empty() {
            len += 1;
        }
        if self.data_type != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_flow.RelationMessageColumn", len)?;
        if self.flags != 0 {
            struct_ser.serialize_field("flags", &self.flags)?;
        }
        if !self.name.is_empty() {
            struct_ser.serialize_field("name", &self.name)?;
        }
        if self.data_type != 0 {
            struct_ser.serialize_field("dataType", &self.data_type)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for RelationMessageColumn {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "flags",
            "name",
            "data_type",
            "dataType",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Flags,
            Name,
            DataType,
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
                            "flags" => Ok(GeneratedField::Flags),
                            "name" => Ok(GeneratedField::Name),
                            "dataType" | "data_type" => Ok(GeneratedField::DataType),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = RelationMessageColumn;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_flow.RelationMessageColumn")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<RelationMessageColumn, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut flags__ = None;
                let mut name__ = None;
                let mut data_type__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Flags => {
                            if flags__.is_some() {
                                return Err(serde::de::Error::duplicate_field("flags"));
                            }
                            flags__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = Some(map.next_value()?);
                        }
                        GeneratedField::DataType => {
                            if data_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("dataType"));
                            }
                            data_type__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(RelationMessageColumn {
                    flags: flags__.unwrap_or_default(),
                    name: name__.unwrap_or_default(),
                    data_type: data_type__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_flow.RelationMessageColumn", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for RenameTableOption {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.current_name.is_empty() {
            len += 1;
        }
        if !self.new_name.is_empty() {
            len += 1;
        }
        if self.table_schema.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_flow.RenameTableOption", len)?;
        if !self.current_name.is_empty() {
            struct_ser.serialize_field("currentName", &self.current_name)?;
        }
        if !self.new_name.is_empty() {
            struct_ser.serialize_field("newName", &self.new_name)?;
        }
        if let Some(v) = self.table_schema.as_ref() {
            struct_ser.serialize_field("tableSchema", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for RenameTableOption {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "current_name",
            "currentName",
            "new_name",
            "newName",
            "table_schema",
            "tableSchema",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            CurrentName,
            NewName,
            TableSchema,
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
                            "currentName" | "current_name" => Ok(GeneratedField::CurrentName),
                            "newName" | "new_name" => Ok(GeneratedField::NewName),
                            "tableSchema" | "table_schema" => Ok(GeneratedField::TableSchema),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = RenameTableOption;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_flow.RenameTableOption")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<RenameTableOption, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut current_name__ = None;
                let mut new_name__ = None;
                let mut table_schema__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::CurrentName => {
                            if current_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("currentName"));
                            }
                            current_name__ = Some(map.next_value()?);
                        }
                        GeneratedField::NewName => {
                            if new_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("newName"));
                            }
                            new_name__ = Some(map.next_value()?);
                        }
                        GeneratedField::TableSchema => {
                            if table_schema__.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableSchema"));
                            }
                            table_schema__ = map.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(RenameTableOption {
                    current_name: current_name__.unwrap_or_default(),
                    new_name: new_name__.unwrap_or_default(),
                    table_schema: table_schema__,
                })
            }
        }
        deserializer.deserialize_struct("peerdb_flow.RenameTableOption", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for RenameTablesInput {
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
        if self.peer.is_some() {
            len += 1;
        }
        if !self.rename_table_options.is_empty() {
            len += 1;
        }
        if self.soft_delete_col_name.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_flow.RenameTablesInput", len)?;
        if !self.flow_job_name.is_empty() {
            struct_ser.serialize_field("flowJobName", &self.flow_job_name)?;
        }
        if let Some(v) = self.peer.as_ref() {
            struct_ser.serialize_field("peer", v)?;
        }
        if !self.rename_table_options.is_empty() {
            struct_ser.serialize_field("renameTableOptions", &self.rename_table_options)?;
        }
        if let Some(v) = self.soft_delete_col_name.as_ref() {
            struct_ser.serialize_field("softDeleteColName", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for RenameTablesInput {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "flow_job_name",
            "flowJobName",
            "peer",
            "rename_table_options",
            "renameTableOptions",
            "soft_delete_col_name",
            "softDeleteColName",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            FlowJobName,
            Peer,
            RenameTableOptions,
            SoftDeleteColName,
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
                            "peer" => Ok(GeneratedField::Peer),
                            "renameTableOptions" | "rename_table_options" => Ok(GeneratedField::RenameTableOptions),
                            "softDeleteColName" | "soft_delete_col_name" => Ok(GeneratedField::SoftDeleteColName),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = RenameTablesInput;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_flow.RenameTablesInput")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<RenameTablesInput, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut flow_job_name__ = None;
                let mut peer__ = None;
                let mut rename_table_options__ = None;
                let mut soft_delete_col_name__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::FlowJobName => {
                            if flow_job_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("flowJobName"));
                            }
                            flow_job_name__ = Some(map.next_value()?);
                        }
                        GeneratedField::Peer => {
                            if peer__.is_some() {
                                return Err(serde::de::Error::duplicate_field("peer"));
                            }
                            peer__ = map.next_value()?;
                        }
                        GeneratedField::RenameTableOptions => {
                            if rename_table_options__.is_some() {
                                return Err(serde::de::Error::duplicate_field("renameTableOptions"));
                            }
                            rename_table_options__ = Some(map.next_value()?);
                        }
                        GeneratedField::SoftDeleteColName => {
                            if soft_delete_col_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("softDeleteColName"));
                            }
                            soft_delete_col_name__ = map.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(RenameTablesInput {
                    flow_job_name: flow_job_name__.unwrap_or_default(),
                    peer: peer__,
                    rename_table_options: rename_table_options__.unwrap_or_default(),
                    soft_delete_col_name: soft_delete_col_name__,
                })
            }
        }
        deserializer.deserialize_struct("peerdb_flow.RenameTablesInput", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for RenameTablesOutput {
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
        let mut struct_ser = serializer.serialize_struct("peerdb_flow.RenameTablesOutput", len)?;
        if !self.flow_job_name.is_empty() {
            struct_ser.serialize_field("flowJobName", &self.flow_job_name)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for RenameTablesOutput {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "flow_job_name",
            "flowJobName",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
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
            type Value = RenameTablesOutput;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_flow.RenameTablesOutput")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<RenameTablesOutput, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut flow_job_name__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
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
                Ok(RenameTablesOutput {
                    flow_job_name: flow_job_name__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_flow.RenameTablesOutput", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ReplayTableSchemaDeltaInput {
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
        if !self.table_schema_deltas.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_flow.ReplayTableSchemaDeltaInput", len)?;
        if let Some(v) = self.flow_connection_configs.as_ref() {
            struct_ser.serialize_field("flowConnectionConfigs", v)?;
        }
        if !self.table_schema_deltas.is_empty() {
            struct_ser.serialize_field("tableSchemaDeltas", &self.table_schema_deltas)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ReplayTableSchemaDeltaInput {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "flow_connection_configs",
            "flowConnectionConfigs",
            "table_schema_deltas",
            "tableSchemaDeltas",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            FlowConnectionConfigs,
            TableSchemaDeltas,
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
                            "tableSchemaDeltas" | "table_schema_deltas" => Ok(GeneratedField::TableSchemaDeltas),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ReplayTableSchemaDeltaInput;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_flow.ReplayTableSchemaDeltaInput")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ReplayTableSchemaDeltaInput, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut flow_connection_configs__ = None;
                let mut table_schema_deltas__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::FlowConnectionConfigs => {
                            if flow_connection_configs__.is_some() {
                                return Err(serde::de::Error::duplicate_field("flowConnectionConfigs"));
                            }
                            flow_connection_configs__ = map.next_value()?;
                        }
                        GeneratedField::TableSchemaDeltas => {
                            if table_schema_deltas__.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableSchemaDeltas"));
                            }
                            table_schema_deltas__ = Some(map.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(ReplayTableSchemaDeltaInput {
                    flow_connection_configs: flow_connection_configs__,
                    table_schema_deltas: table_schema_deltas__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_flow.ReplayTableSchemaDeltaInput", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SetupNormalizedTableBatchInput {
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
        if !self.table_name_schema_mapping.is_empty() {
            len += 1;
        }
        if !self.soft_delete_col_name.is_empty() {
            len += 1;
        }
        if !self.synced_at_col_name.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_flow.SetupNormalizedTableBatchInput", len)?;
        if let Some(v) = self.peer_connection_config.as_ref() {
            struct_ser.serialize_field("peerConnectionConfig", v)?;
        }
        if !self.table_name_schema_mapping.is_empty() {
            struct_ser.serialize_field("tableNameSchemaMapping", &self.table_name_schema_mapping)?;
        }
        if !self.soft_delete_col_name.is_empty() {
            struct_ser.serialize_field("softDeleteColName", &self.soft_delete_col_name)?;
        }
        if !self.synced_at_col_name.is_empty() {
            struct_ser.serialize_field("syncedAtColName", &self.synced_at_col_name)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SetupNormalizedTableBatchInput {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "peer_connection_config",
            "peerConnectionConfig",
            "table_name_schema_mapping",
            "tableNameSchemaMapping",
            "soft_delete_col_name",
            "softDeleteColName",
            "synced_at_col_name",
            "syncedAtColName",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            PeerConnectionConfig,
            TableNameSchemaMapping,
            SoftDeleteColName,
            SyncedAtColName,
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
                            "tableNameSchemaMapping" | "table_name_schema_mapping" => Ok(GeneratedField::TableNameSchemaMapping),
                            "softDeleteColName" | "soft_delete_col_name" => Ok(GeneratedField::SoftDeleteColName),
                            "syncedAtColName" | "synced_at_col_name" => Ok(GeneratedField::SyncedAtColName),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SetupNormalizedTableBatchInput;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_flow.SetupNormalizedTableBatchInput")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<SetupNormalizedTableBatchInput, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut peer_connection_config__ = None;
                let mut table_name_schema_mapping__ = None;
                let mut soft_delete_col_name__ = None;
                let mut synced_at_col_name__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::PeerConnectionConfig => {
                            if peer_connection_config__.is_some() {
                                return Err(serde::de::Error::duplicate_field("peerConnectionConfig"));
                            }
                            peer_connection_config__ = map.next_value()?;
                        }
                        GeneratedField::TableNameSchemaMapping => {
                            if table_name_schema_mapping__.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableNameSchemaMapping"));
                            }
                            table_name_schema_mapping__ = Some(
                                map.next_value::<std::collections::HashMap<_, _>>()?
                            );
                        }
                        GeneratedField::SoftDeleteColName => {
                            if soft_delete_col_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("softDeleteColName"));
                            }
                            soft_delete_col_name__ = Some(map.next_value()?);
                        }
                        GeneratedField::SyncedAtColName => {
                            if synced_at_col_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("syncedAtColName"));
                            }
                            synced_at_col_name__ = Some(map.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(SetupNormalizedTableBatchInput {
                    peer_connection_config: peer_connection_config__,
                    table_name_schema_mapping: table_name_schema_mapping__.unwrap_or_default(),
                    soft_delete_col_name: soft_delete_col_name__.unwrap_or_default(),
                    synced_at_col_name: synced_at_col_name__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_flow.SetupNormalizedTableBatchInput", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SetupNormalizedTableBatchOutput {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.table_exists_mapping.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_flow.SetupNormalizedTableBatchOutput", len)?;
        if !self.table_exists_mapping.is_empty() {
            struct_ser.serialize_field("tableExistsMapping", &self.table_exists_mapping)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SetupNormalizedTableBatchOutput {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "table_exists_mapping",
            "tableExistsMapping",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            TableExistsMapping,
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
                            "tableExistsMapping" | "table_exists_mapping" => Ok(GeneratedField::TableExistsMapping),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SetupNormalizedTableBatchOutput;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_flow.SetupNormalizedTableBatchOutput")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<SetupNormalizedTableBatchOutput, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut table_exists_mapping__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::TableExistsMapping => {
                            if table_exists_mapping__.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableExistsMapping"));
                            }
                            table_exists_mapping__ = Some(
                                map.next_value::<std::collections::HashMap<_, _>>()?
                            );
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(SetupNormalizedTableBatchOutput {
                    table_exists_mapping: table_exists_mapping__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_flow.SetupNormalizedTableBatchOutput", FIELDS, GeneratedVisitor)
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
        if self.do_initial_copy {
            len += 1;
        }
        if !self.existing_publication_name.is_empty() {
            len += 1;
        }
        if !self.existing_replication_slot_name.is_empty() {
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
        if self.do_initial_copy {
            struct_ser.serialize_field("doInitialCopy", &self.do_initial_copy)?;
        }
        if !self.existing_publication_name.is_empty() {
            struct_ser.serialize_field("existingPublicationName", &self.existing_publication_name)?;
        }
        if !self.existing_replication_slot_name.is_empty() {
            struct_ser.serialize_field("existingReplicationSlotName", &self.existing_replication_slot_name)?;
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
            "do_initial_copy",
            "doInitialCopy",
            "existing_publication_name",
            "existingPublicationName",
            "existing_replication_slot_name",
            "existingReplicationSlotName",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            PeerConnectionConfig,
            FlowJobName,
            TableNameMapping,
            DestinationPeer,
            DoInitialCopy,
            ExistingPublicationName,
            ExistingReplicationSlotName,
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
                            "doInitialCopy" | "do_initial_copy" => Ok(GeneratedField::DoInitialCopy),
                            "existingPublicationName" | "existing_publication_name" => Ok(GeneratedField::ExistingPublicationName),
                            "existingReplicationSlotName" | "existing_replication_slot_name" => Ok(GeneratedField::ExistingReplicationSlotName),
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
                let mut do_initial_copy__ = None;
                let mut existing_publication_name__ = None;
                let mut existing_replication_slot_name__ = None;
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
                        GeneratedField::DoInitialCopy => {
                            if do_initial_copy__.is_some() {
                                return Err(serde::de::Error::duplicate_field("doInitialCopy"));
                            }
                            do_initial_copy__ = Some(map.next_value()?);
                        }
                        GeneratedField::ExistingPublicationName => {
                            if existing_publication_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("existingPublicationName"));
                            }
                            existing_publication_name__ = Some(map.next_value()?);
                        }
                        GeneratedField::ExistingReplicationSlotName => {
                            if existing_replication_slot_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("existingReplicationSlotName"));
                            }
                            existing_replication_slot_name__ = Some(map.next_value()?);
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
                    do_initial_copy: do_initial_copy__.unwrap_or_default(),
                    existing_publication_name: existing_publication_name__.unwrap_or_default(),
                    existing_replication_slot_name: existing_replication_slot_name__.unwrap_or_default(),
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
        if !self.relation_message_mapping.is_empty() {
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
        if !self.relation_message_mapping.is_empty() {
            struct_ser.serialize_field("relationMessageMapping", &self.relation_message_mapping)?;
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
            "relation_message_mapping",
            "relationMessageMapping",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            LastSyncState,
            FlowConnectionConfigs,
            SyncFlowOptions,
            RelationMessageMapping,
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
                            "relationMessageMapping" | "relation_message_mapping" => Ok(GeneratedField::RelationMessageMapping),
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
                let mut relation_message_mapping__ = None;
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
                        GeneratedField::RelationMessageMapping => {
                            if relation_message_mapping__.is_some() {
                                return Err(serde::de::Error::duplicate_field("relationMessageMapping"));
                            }
                            relation_message_mapping__ = Some(
                                map.next_value::<std::collections::HashMap<::pbjson::private::NumberDeserialize<u32>, _>>()?
                                    .into_iter().map(|(k,v)| (k.0, v)).collect()
                            );
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
                    relation_message_mapping: relation_message_mapping__.unwrap_or_default(),
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
        if !self.relation_message_mapping.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_flow.SyncFlowOptions", len)?;
        if self.batch_size != 0 {
            struct_ser.serialize_field("batchSize", &self.batch_size)?;
        }
        if !self.relation_message_mapping.is_empty() {
            struct_ser.serialize_field("relationMessageMapping", &self.relation_message_mapping)?;
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
            "relation_message_mapping",
            "relationMessageMapping",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            BatchSize,
            RelationMessageMapping,
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
                            "relationMessageMapping" | "relation_message_mapping" => Ok(GeneratedField::RelationMessageMapping),
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
                let mut relation_message_mapping__ = None;
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
                        GeneratedField::RelationMessageMapping => {
                            if relation_message_mapping__.is_some() {
                                return Err(serde::de::Error::duplicate_field("relationMessageMapping"));
                            }
                            relation_message_mapping__ = Some(
                                map.next_value::<std::collections::HashMap<::pbjson::private::NumberDeserialize<u32>, _>>()?
                                    .into_iter().map(|(k,v)| (k.0, v)).collect()
                            );
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(SyncFlowOptions {
                    batch_size: batch_size__.unwrap_or_default(),
                    relation_message_mapping: relation_message_mapping__.unwrap_or_default(),
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
impl serde::Serialize for TableMapping {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.source_table_identifier.is_empty() {
            len += 1;
        }
        if !self.destination_table_identifier.is_empty() {
            len += 1;
        }
        if !self.partition_key.is_empty() {
            len += 1;
        }
        if !self.exclude.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_flow.TableMapping", len)?;
        if !self.source_table_identifier.is_empty() {
            struct_ser.serialize_field("sourceTableIdentifier", &self.source_table_identifier)?;
        }
        if !self.destination_table_identifier.is_empty() {
            struct_ser.serialize_field("destinationTableIdentifier", &self.destination_table_identifier)?;
        }
        if !self.partition_key.is_empty() {
            struct_ser.serialize_field("partitionKey", &self.partition_key)?;
        }
        if !self.exclude.is_empty() {
            struct_ser.serialize_field("exclude", &self.exclude)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for TableMapping {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "source_table_identifier",
            "sourceTableIdentifier",
            "destination_table_identifier",
            "destinationTableIdentifier",
            "partition_key",
            "partitionKey",
            "exclude",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            SourceTableIdentifier,
            DestinationTableIdentifier,
            PartitionKey,
            Exclude,
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
                            "sourceTableIdentifier" | "source_table_identifier" => Ok(GeneratedField::SourceTableIdentifier),
                            "destinationTableIdentifier" | "destination_table_identifier" => Ok(GeneratedField::DestinationTableIdentifier),
                            "partitionKey" | "partition_key" => Ok(GeneratedField::PartitionKey),
                            "exclude" => Ok(GeneratedField::Exclude),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = TableMapping;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_flow.TableMapping")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<TableMapping, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut source_table_identifier__ = None;
                let mut destination_table_identifier__ = None;
                let mut partition_key__ = None;
                let mut exclude__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::SourceTableIdentifier => {
                            if source_table_identifier__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sourceTableIdentifier"));
                            }
                            source_table_identifier__ = Some(map.next_value()?);
                        }
                        GeneratedField::DestinationTableIdentifier => {
                            if destination_table_identifier__.is_some() {
                                return Err(serde::de::Error::duplicate_field("destinationTableIdentifier"));
                            }
                            destination_table_identifier__ = Some(map.next_value()?);
                        }
                        GeneratedField::PartitionKey => {
                            if partition_key__.is_some() {
                                return Err(serde::de::Error::duplicate_field("partitionKey"));
                            }
                            partition_key__ = Some(map.next_value()?);
                        }
                        GeneratedField::Exclude => {
                            if exclude__.is_some() {
                                return Err(serde::de::Error::duplicate_field("exclude"));
                            }
                            exclude__ = Some(map.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(TableMapping {
                    source_table_identifier: source_table_identifier__.unwrap_or_default(),
                    destination_table_identifier: destination_table_identifier__.unwrap_or_default(),
                    partition_key: partition_key__.unwrap_or_default(),
                    exclude: exclude__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_flow.TableMapping", FIELDS, GeneratedVisitor)
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
        if !self.primary_key_columns.is_empty() {
            len += 1;
        }
        if self.is_replica_identity_full {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_flow.TableSchema", len)?;
        if !self.table_identifier.is_empty() {
            struct_ser.serialize_field("tableIdentifier", &self.table_identifier)?;
        }
        if !self.columns.is_empty() {
            struct_ser.serialize_field("columns", &self.columns)?;
        }
        if !self.primary_key_columns.is_empty() {
            struct_ser.serialize_field("primaryKeyColumns", &self.primary_key_columns)?;
        }
        if self.is_replica_identity_full {
            struct_ser.serialize_field("isReplicaIdentityFull", &self.is_replica_identity_full)?;
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
            "primary_key_columns",
            "primaryKeyColumns",
            "is_replica_identity_full",
            "isReplicaIdentityFull",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            TableIdentifier,
            Columns,
            PrimaryKeyColumns,
            IsReplicaIdentityFull,
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
                            "primaryKeyColumns" | "primary_key_columns" => Ok(GeneratedField::PrimaryKeyColumns),
                            "isReplicaIdentityFull" | "is_replica_identity_full" => Ok(GeneratedField::IsReplicaIdentityFull),
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
                let mut primary_key_columns__ = None;
                let mut is_replica_identity_full__ = None;
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
                        GeneratedField::PrimaryKeyColumns => {
                            if primary_key_columns__.is_some() {
                                return Err(serde::de::Error::duplicate_field("primaryKeyColumns"));
                            }
                            primary_key_columns__ = Some(map.next_value()?);
                        }
                        GeneratedField::IsReplicaIdentityFull => {
                            if is_replica_identity_full__.is_some() {
                                return Err(serde::de::Error::duplicate_field("isReplicaIdentityFull"));
                            }
                            is_replica_identity_full__ = Some(map.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(TableSchema {
                    table_identifier: table_identifier__.unwrap_or_default(),
                    columns: columns__.unwrap_or_default(),
                    primary_key_columns: primary_key_columns__.unwrap_or_default(),
                    is_replica_identity_full: is_replica_identity_full__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_flow.TableSchema", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for TableSchemaDelta {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.src_table_name.is_empty() {
            len += 1;
        }
        if !self.dst_table_name.is_empty() {
            len += 1;
        }
        if !self.added_columns.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_flow.TableSchemaDelta", len)?;
        if !self.src_table_name.is_empty() {
            struct_ser.serialize_field("srcTableName", &self.src_table_name)?;
        }
        if !self.dst_table_name.is_empty() {
            struct_ser.serialize_field("dstTableName", &self.dst_table_name)?;
        }
        if !self.added_columns.is_empty() {
            struct_ser.serialize_field("addedColumns", &self.added_columns)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for TableSchemaDelta {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "src_table_name",
            "srcTableName",
            "dst_table_name",
            "dstTableName",
            "added_columns",
            "addedColumns",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            SrcTableName,
            DstTableName,
            AddedColumns,
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
                            "srcTableName" | "src_table_name" => Ok(GeneratedField::SrcTableName),
                            "dstTableName" | "dst_table_name" => Ok(GeneratedField::DstTableName),
                            "addedColumns" | "added_columns" => Ok(GeneratedField::AddedColumns),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = TableSchemaDelta;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_flow.TableSchemaDelta")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<TableSchemaDelta, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut src_table_name__ = None;
                let mut dst_table_name__ = None;
                let mut added_columns__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::SrcTableName => {
                            if src_table_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("srcTableName"));
                            }
                            src_table_name__ = Some(map.next_value()?);
                        }
                        GeneratedField::DstTableName => {
                            if dst_table_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("dstTableName"));
                            }
                            dst_table_name__ = Some(map.next_value()?);
                        }
                        GeneratedField::AddedColumns => {
                            if added_columns__.is_some() {
                                return Err(serde::de::Error::duplicate_field("addedColumns"));
                            }
                            added_columns__ = Some(map.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(TableSchemaDelta {
                    src_table_name: src_table_name__.unwrap_or_default(),
                    dst_table_name: dst_table_name__.unwrap_or_default(),
                    added_columns: added_columns__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_flow.TableSchemaDelta", FIELDS, GeneratedVisitor)
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
