// @generated
impl serde::Serialize for CdcMirrorStatus {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.config.is_some() {
            len += 1;
        }
        if self.snapshot_status.is_some() {
            len += 1;
        }
        if !self.cdc_syncs.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_route.CDCMirrorStatus", len)?;
        if let Some(v) = self.config.as_ref() {
            struct_ser.serialize_field("config", v)?;
        }
        if let Some(v) = self.snapshot_status.as_ref() {
            struct_ser.serialize_field("snapshotStatus", v)?;
        }
        if !self.cdc_syncs.is_empty() {
            struct_ser.serialize_field("cdcSyncs", &self.cdc_syncs)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CdcMirrorStatus {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "config",
            "snapshot_status",
            "snapshotStatus",
            "cdc_syncs",
            "cdcSyncs",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Config,
            SnapshotStatus,
            CdcSyncs,
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
                            "config" => Ok(GeneratedField::Config),
                            "snapshotStatus" | "snapshot_status" => Ok(GeneratedField::SnapshotStatus),
                            "cdcSyncs" | "cdc_syncs" => Ok(GeneratedField::CdcSyncs),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CdcMirrorStatus;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_route.CDCMirrorStatus")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CdcMirrorStatus, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut config__ = None;
                let mut snapshot_status__ = None;
                let mut cdc_syncs__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Config => {
                            if config__.is_some() {
                                return Err(serde::de::Error::duplicate_field("config"));
                            }
                            config__ = map.next_value()?;
                        }
                        GeneratedField::SnapshotStatus => {
                            if snapshot_status__.is_some() {
                                return Err(serde::de::Error::duplicate_field("snapshotStatus"));
                            }
                            snapshot_status__ = map.next_value()?;
                        }
                        GeneratedField::CdcSyncs => {
                            if cdc_syncs__.is_some() {
                                return Err(serde::de::Error::duplicate_field("cdcSyncs"));
                            }
                            cdc_syncs__ = Some(map.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(CdcMirrorStatus {
                    config: config__,
                    snapshot_status: snapshot_status__,
                    cdc_syncs: cdc_syncs__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_route.CDCMirrorStatus", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CdcSyncStatus {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.start_lsn != 0 {
            len += 1;
        }
        if self.end_lsn != 0 {
            len += 1;
        }
        if self.num_rows != 0 {
            len += 1;
        }
        if self.start_time.is_some() {
            len += 1;
        }
        if self.end_time.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_route.CDCSyncStatus", len)?;
        if self.start_lsn != 0 {
            struct_ser.serialize_field("startLsn", ToString::to_string(&self.start_lsn).as_str())?;
        }
        if self.end_lsn != 0 {
            struct_ser.serialize_field("endLsn", ToString::to_string(&self.end_lsn).as_str())?;
        }
        if self.num_rows != 0 {
            struct_ser.serialize_field("numRows", &self.num_rows)?;
        }
        if let Some(v) = self.start_time.as_ref() {
            struct_ser.serialize_field("startTime", v)?;
        }
        if let Some(v) = self.end_time.as_ref() {
            struct_ser.serialize_field("endTime", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CdcSyncStatus {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "start_lsn",
            "startLsn",
            "end_lsn",
            "endLsn",
            "num_rows",
            "numRows",
            "start_time",
            "startTime",
            "end_time",
            "endTime",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            StartLsn,
            EndLsn,
            NumRows,
            StartTime,
            EndTime,
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
                            "startLsn" | "start_lsn" => Ok(GeneratedField::StartLsn),
                            "endLsn" | "end_lsn" => Ok(GeneratedField::EndLsn),
                            "numRows" | "num_rows" => Ok(GeneratedField::NumRows),
                            "startTime" | "start_time" => Ok(GeneratedField::StartTime),
                            "endTime" | "end_time" => Ok(GeneratedField::EndTime),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CdcSyncStatus;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_route.CDCSyncStatus")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CdcSyncStatus, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut start_lsn__ = None;
                let mut end_lsn__ = None;
                let mut num_rows__ = None;
                let mut start_time__ = None;
                let mut end_time__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::StartLsn => {
                            if start_lsn__.is_some() {
                                return Err(serde::de::Error::duplicate_field("startLsn"));
                            }
                            start_lsn__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::EndLsn => {
                            if end_lsn__.is_some() {
                                return Err(serde::de::Error::duplicate_field("endLsn"));
                            }
                            end_lsn__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::NumRows => {
                            if num_rows__.is_some() {
                                return Err(serde::de::Error::duplicate_field("numRows"));
                            }
                            num_rows__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::StartTime => {
                            if start_time__.is_some() {
                                return Err(serde::de::Error::duplicate_field("startTime"));
                            }
                            start_time__ = map.next_value()?;
                        }
                        GeneratedField::EndTime => {
                            if end_time__.is_some() {
                                return Err(serde::de::Error::duplicate_field("endTime"));
                            }
                            end_time__ = map.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(CdcSyncStatus {
                    start_lsn: start_lsn__.unwrap_or_default(),
                    end_lsn: end_lsn__.unwrap_or_default(),
                    num_rows: num_rows__.unwrap_or_default(),
                    start_time: start_time__,
                    end_time: end_time__,
                })
            }
        }
        deserializer.deserialize_struct("peerdb_route.CDCSyncStatus", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CreateCdcFlowRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.connection_configs.is_some() {
            len += 1;
        }
        if self.create_catalog_entry {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_route.CreateCDCFlowRequest", len)?;
        if let Some(v) = self.connection_configs.as_ref() {
            struct_ser.serialize_field("connectionConfigs", v)?;
        }
        if self.create_catalog_entry {
            struct_ser.serialize_field("createCatalogEntry", &self.create_catalog_entry)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CreateCdcFlowRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "connection_configs",
            "connectionConfigs",
            "create_catalog_entry",
            "createCatalogEntry",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ConnectionConfigs,
            CreateCatalogEntry,
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
                            "connectionConfigs" | "connection_configs" => Ok(GeneratedField::ConnectionConfigs),
                            "createCatalogEntry" | "create_catalog_entry" => Ok(GeneratedField::CreateCatalogEntry),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CreateCdcFlowRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_route.CreateCDCFlowRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CreateCdcFlowRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut connection_configs__ = None;
                let mut create_catalog_entry__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::ConnectionConfigs => {
                            if connection_configs__.is_some() {
                                return Err(serde::de::Error::duplicate_field("connectionConfigs"));
                            }
                            connection_configs__ = map.next_value()?;
                        }
                        GeneratedField::CreateCatalogEntry => {
                            if create_catalog_entry__.is_some() {
                                return Err(serde::de::Error::duplicate_field("createCatalogEntry"));
                            }
                            create_catalog_entry__ = Some(map.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(CreateCdcFlowRequest {
                    connection_configs: connection_configs__,
                    create_catalog_entry: create_catalog_entry__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_route.CreateCDCFlowRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CreateCdcFlowResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.worflow_id.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_route.CreateCDCFlowResponse", len)?;
        if !self.worflow_id.is_empty() {
            struct_ser.serialize_field("worflowId", &self.worflow_id)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CreateCdcFlowResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "worflow_id",
            "worflowId",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            WorflowId,
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
                            "worflowId" | "worflow_id" => Ok(GeneratedField::WorflowId),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CreateCdcFlowResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_route.CreateCDCFlowResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CreateCdcFlowResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut worflow_id__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::WorflowId => {
                            if worflow_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("worflowId"));
                            }
                            worflow_id__ = Some(map.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(CreateCdcFlowResponse {
                    worflow_id: worflow_id__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_route.CreateCDCFlowResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CreatePeerRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.peer.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_route.CreatePeerRequest", len)?;
        if let Some(v) = self.peer.as_ref() {
            struct_ser.serialize_field("peer", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CreatePeerRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "peer",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Peer,
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
                            "peer" => Ok(GeneratedField::Peer),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CreatePeerRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_route.CreatePeerRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CreatePeerRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut peer__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Peer => {
                            if peer__.is_some() {
                                return Err(serde::de::Error::duplicate_field("peer"));
                            }
                            peer__ = map.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(CreatePeerRequest {
                    peer: peer__,
                })
            }
        }
        deserializer.deserialize_struct("peerdb_route.CreatePeerRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CreatePeerResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.status != 0 {
            len += 1;
        }
        if !self.message.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_route.CreatePeerResponse", len)?;
        if self.status != 0 {
            let v = CreatePeerStatus::from_i32(self.status)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.status)))?;
            struct_ser.serialize_field("status", &v)?;
        }
        if !self.message.is_empty() {
            struct_ser.serialize_field("message", &self.message)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CreatePeerResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "status",
            "message",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Status,
            Message,
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
                            "status" => Ok(GeneratedField::Status),
                            "message" => Ok(GeneratedField::Message),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CreatePeerResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_route.CreatePeerResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CreatePeerResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut status__ = None;
                let mut message__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Status => {
                            if status__.is_some() {
                                return Err(serde::de::Error::duplicate_field("status"));
                            }
                            status__ = Some(map.next_value::<CreatePeerStatus>()? as i32);
                        }
                        GeneratedField::Message => {
                            if message__.is_some() {
                                return Err(serde::de::Error::duplicate_field("message"));
                            }
                            message__ = Some(map.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(CreatePeerResponse {
                    status: status__.unwrap_or_default(),
                    message: message__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_route.CreatePeerResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CreatePeerStatus {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::ValidationUnknown => "VALIDATION_UNKNOWN",
            Self::Created => "CREATED",
            Self::Failed => "FAILED",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for CreatePeerStatus {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "VALIDATION_UNKNOWN",
            "CREATED",
            "FAILED",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CreatePeerStatus;

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
                    .and_then(CreatePeerStatus::from_i32)
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
                    .and_then(CreatePeerStatus::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "VALIDATION_UNKNOWN" => Ok(CreatePeerStatus::ValidationUnknown),
                    "CREATED" => Ok(CreatePeerStatus::Created),
                    "FAILED" => Ok(CreatePeerStatus::Failed),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for CreateQRepFlowRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.qrep_config.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_route.CreateQRepFlowRequest", len)?;
        if let Some(v) = self.qrep_config.as_ref() {
            struct_ser.serialize_field("qrepConfig", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CreateQRepFlowRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "qrep_config",
            "qrepConfig",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            QrepConfig,
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
                            "qrepConfig" | "qrep_config" => Ok(GeneratedField::QrepConfig),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CreateQRepFlowRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_route.CreateQRepFlowRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CreateQRepFlowRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut qrep_config__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::QrepConfig => {
                            if qrep_config__.is_some() {
                                return Err(serde::de::Error::duplicate_field("qrepConfig"));
                            }
                            qrep_config__ = map.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(CreateQRepFlowRequest {
                    qrep_config: qrep_config__,
                })
            }
        }
        deserializer.deserialize_struct("peerdb_route.CreateQRepFlowRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CreateQRepFlowResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.worflow_id.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_route.CreateQRepFlowResponse", len)?;
        if !self.worflow_id.is_empty() {
            struct_ser.serialize_field("worflowId", &self.worflow_id)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CreateQRepFlowResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "worflow_id",
            "worflowId",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            WorflowId,
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
                            "worflowId" | "worflow_id" => Ok(GeneratedField::WorflowId),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CreateQRepFlowResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_route.CreateQRepFlowResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CreateQRepFlowResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut worflow_id__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::WorflowId => {
                            if worflow_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("worflowId"));
                            }
                            worflow_id__ = Some(map.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(CreateQRepFlowResponse {
                    worflow_id: worflow_id__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_route.CreateQRepFlowResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for MirrorStatusRequest {
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
        let mut struct_ser = serializer.serialize_struct("peerdb_route.MirrorStatusRequest", len)?;
        if !self.flow_job_name.is_empty() {
            struct_ser.serialize_field("flowJobName", &self.flow_job_name)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for MirrorStatusRequest {
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
            type Value = MirrorStatusRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_route.MirrorStatusRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<MirrorStatusRequest, V::Error>
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
                Ok(MirrorStatusRequest {
                    flow_job_name: flow_job_name__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_route.MirrorStatusRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for MirrorStatusResponse {
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
        if !self.error_message.is_empty() {
            len += 1;
        }
        if self.status.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_route.MirrorStatusResponse", len)?;
        if !self.flow_job_name.is_empty() {
            struct_ser.serialize_field("flowJobName", &self.flow_job_name)?;
        }
        if !self.error_message.is_empty() {
            struct_ser.serialize_field("errorMessage", &self.error_message)?;
        }
        if let Some(v) = self.status.as_ref() {
            match v {
                mirror_status_response::Status::QrepStatus(v) => {
                    struct_ser.serialize_field("qrepStatus", v)?;
                }
                mirror_status_response::Status::CdcStatus(v) => {
                    struct_ser.serialize_field("cdcStatus", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for MirrorStatusResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "flow_job_name",
            "flowJobName",
            "error_message",
            "errorMessage",
            "qrep_status",
            "qrepStatus",
            "cdc_status",
            "cdcStatus",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            FlowJobName,
            ErrorMessage,
            QrepStatus,
            CdcStatus,
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
                            "errorMessage" | "error_message" => Ok(GeneratedField::ErrorMessage),
                            "qrepStatus" | "qrep_status" => Ok(GeneratedField::QrepStatus),
                            "cdcStatus" | "cdc_status" => Ok(GeneratedField::CdcStatus),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = MirrorStatusResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_route.MirrorStatusResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<MirrorStatusResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut flow_job_name__ = None;
                let mut error_message__ = None;
                let mut status__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::FlowJobName => {
                            if flow_job_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("flowJobName"));
                            }
                            flow_job_name__ = Some(map.next_value()?);
                        }
                        GeneratedField::ErrorMessage => {
                            if error_message__.is_some() {
                                return Err(serde::de::Error::duplicate_field("errorMessage"));
                            }
                            error_message__ = Some(map.next_value()?);
                        }
                        GeneratedField::QrepStatus => {
                            if status__.is_some() {
                                return Err(serde::de::Error::duplicate_field("qrepStatus"));
                            }
                            status__ = map.next_value::<::std::option::Option<_>>()?.map(mirror_status_response::Status::QrepStatus)
;
                        }
                        GeneratedField::CdcStatus => {
                            if status__.is_some() {
                                return Err(serde::de::Error::duplicate_field("cdcStatus"));
                            }
                            status__ = map.next_value::<::std::option::Option<_>>()?.map(mirror_status_response::Status::CdcStatus)
;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(MirrorStatusResponse {
                    flow_job_name: flow_job_name__.unwrap_or_default(),
                    error_message: error_message__.unwrap_or_default(),
                    status: status__,
                })
            }
        }
        deserializer.deserialize_struct("peerdb_route.MirrorStatusResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PartitionStatus {
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
        if self.start_time.is_some() {
            len += 1;
        }
        if self.end_time.is_some() {
            len += 1;
        }
        if self.num_rows != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_route.PartitionStatus", len)?;
        if !self.partition_id.is_empty() {
            struct_ser.serialize_field("partitionId", &self.partition_id)?;
        }
        if let Some(v) = self.start_time.as_ref() {
            struct_ser.serialize_field("startTime", v)?;
        }
        if let Some(v) = self.end_time.as_ref() {
            struct_ser.serialize_field("endTime", v)?;
        }
        if self.num_rows != 0 {
            struct_ser.serialize_field("numRows", &self.num_rows)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PartitionStatus {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "partition_id",
            "partitionId",
            "start_time",
            "startTime",
            "end_time",
            "endTime",
            "num_rows",
            "numRows",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            PartitionId,
            StartTime,
            EndTime,
            NumRows,
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
                            "startTime" | "start_time" => Ok(GeneratedField::StartTime),
                            "endTime" | "end_time" => Ok(GeneratedField::EndTime),
                            "numRows" | "num_rows" => Ok(GeneratedField::NumRows),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PartitionStatus;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_route.PartitionStatus")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<PartitionStatus, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut partition_id__ = None;
                let mut start_time__ = None;
                let mut end_time__ = None;
                let mut num_rows__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::PartitionId => {
                            if partition_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("partitionId"));
                            }
                            partition_id__ = Some(map.next_value()?);
                        }
                        GeneratedField::StartTime => {
                            if start_time__.is_some() {
                                return Err(serde::de::Error::duplicate_field("startTime"));
                            }
                            start_time__ = map.next_value()?;
                        }
                        GeneratedField::EndTime => {
                            if end_time__.is_some() {
                                return Err(serde::de::Error::duplicate_field("endTime"));
                            }
                            end_time__ = map.next_value()?;
                        }
                        GeneratedField::NumRows => {
                            if num_rows__.is_some() {
                                return Err(serde::de::Error::duplicate_field("numRows"));
                            }
                            num_rows__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(PartitionStatus {
                    partition_id: partition_id__.unwrap_or_default(),
                    start_time: start_time__,
                    end_time: end_time__,
                    num_rows: num_rows__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_route.PartitionStatus", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for QRepMirrorStatus {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.config.is_some() {
            len += 1;
        }
        if !self.partitions.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_route.QRepMirrorStatus", len)?;
        if let Some(v) = self.config.as_ref() {
            struct_ser.serialize_field("config", v)?;
        }
        if !self.partitions.is_empty() {
            struct_ser.serialize_field("partitions", &self.partitions)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for QRepMirrorStatus {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "config",
            "partitions",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Config,
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
                            "config" => Ok(GeneratedField::Config),
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
            type Value = QRepMirrorStatus;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_route.QRepMirrorStatus")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<QRepMirrorStatus, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut config__ = None;
                let mut partitions__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Config => {
                            if config__.is_some() {
                                return Err(serde::de::Error::duplicate_field("config"));
                            }
                            config__ = map.next_value()?;
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
                Ok(QRepMirrorStatus {
                    config: config__,
                    partitions: partitions__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_route.QRepMirrorStatus", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ShutdownRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.workflow_id.is_empty() {
            len += 1;
        }
        if !self.flow_job_name.is_empty() {
            len += 1;
        }
        if self.source_peer.is_some() {
            len += 1;
        }
        if self.destination_peer.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_route.ShutdownRequest", len)?;
        if !self.workflow_id.is_empty() {
            struct_ser.serialize_field("workflowId", &self.workflow_id)?;
        }
        if !self.flow_job_name.is_empty() {
            struct_ser.serialize_field("flowJobName", &self.flow_job_name)?;
        }
        if let Some(v) = self.source_peer.as_ref() {
            struct_ser.serialize_field("sourcePeer", v)?;
        }
        if let Some(v) = self.destination_peer.as_ref() {
            struct_ser.serialize_field("destinationPeer", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ShutdownRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "workflow_id",
            "workflowId",
            "flow_job_name",
            "flowJobName",
            "source_peer",
            "sourcePeer",
            "destination_peer",
            "destinationPeer",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            WorkflowId,
            FlowJobName,
            SourcePeer,
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
                            "workflowId" | "workflow_id" => Ok(GeneratedField::WorkflowId),
                            "flowJobName" | "flow_job_name" => Ok(GeneratedField::FlowJobName),
                            "sourcePeer" | "source_peer" => Ok(GeneratedField::SourcePeer),
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
            type Value = ShutdownRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_route.ShutdownRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ShutdownRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut workflow_id__ = None;
                let mut flow_job_name__ = None;
                let mut source_peer__ = None;
                let mut destination_peer__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::WorkflowId => {
                            if workflow_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("workflowId"));
                            }
                            workflow_id__ = Some(map.next_value()?);
                        }
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
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(ShutdownRequest {
                    workflow_id: workflow_id__.unwrap_or_default(),
                    flow_job_name: flow_job_name__.unwrap_or_default(),
                    source_peer: source_peer__,
                    destination_peer: destination_peer__,
                })
            }
        }
        deserializer.deserialize_struct("peerdb_route.ShutdownRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ShutdownResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.ok {
            len += 1;
        }
        if !self.error_message.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_route.ShutdownResponse", len)?;
        if self.ok {
            struct_ser.serialize_field("ok", &self.ok)?;
        }
        if !self.error_message.is_empty() {
            struct_ser.serialize_field("errorMessage", &self.error_message)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ShutdownResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "ok",
            "error_message",
            "errorMessage",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Ok,
            ErrorMessage,
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
                            "ok" => Ok(GeneratedField::Ok),
                            "errorMessage" | "error_message" => Ok(GeneratedField::ErrorMessage),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ShutdownResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_route.ShutdownResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ShutdownResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut ok__ = None;
                let mut error_message__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Ok => {
                            if ok__.is_some() {
                                return Err(serde::de::Error::duplicate_field("ok"));
                            }
                            ok__ = Some(map.next_value()?);
                        }
                        GeneratedField::ErrorMessage => {
                            if error_message__.is_some() {
                                return Err(serde::de::Error::duplicate_field("errorMessage"));
                            }
                            error_message__ = Some(map.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(ShutdownResponse {
                    ok: ok__.unwrap_or_default(),
                    error_message: error_message__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_route.ShutdownResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SnapshotStatus {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.clones.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_route.SnapshotStatus", len)?;
        if !self.clones.is_empty() {
            struct_ser.serialize_field("clones", &self.clones)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SnapshotStatus {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "clones",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Clones,
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
                            "clones" => Ok(GeneratedField::Clones),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SnapshotStatus;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_route.SnapshotStatus")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<SnapshotStatus, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut clones__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Clones => {
                            if clones__.is_some() {
                                return Err(serde::de::Error::duplicate_field("clones"));
                            }
                            clones__ = Some(map.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(SnapshotStatus {
                    clones: clones__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_route.SnapshotStatus", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ValidatePeerRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.peer.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_route.ValidatePeerRequest", len)?;
        if let Some(v) = self.peer.as_ref() {
            struct_ser.serialize_field("peer", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ValidatePeerRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "peer",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Peer,
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
                            "peer" => Ok(GeneratedField::Peer),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ValidatePeerRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_route.ValidatePeerRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ValidatePeerRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut peer__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Peer => {
                            if peer__.is_some() {
                                return Err(serde::de::Error::duplicate_field("peer"));
                            }
                            peer__ = map.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(ValidatePeerRequest {
                    peer: peer__,
                })
            }
        }
        deserializer.deserialize_struct("peerdb_route.ValidatePeerRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ValidatePeerResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.status != 0 {
            len += 1;
        }
        if !self.message.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_route.ValidatePeerResponse", len)?;
        if self.status != 0 {
            let v = ValidatePeerStatus::from_i32(self.status)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.status)))?;
            struct_ser.serialize_field("status", &v)?;
        }
        if !self.message.is_empty() {
            struct_ser.serialize_field("message", &self.message)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ValidatePeerResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "status",
            "message",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Status,
            Message,
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
                            "status" => Ok(GeneratedField::Status),
                            "message" => Ok(GeneratedField::Message),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ValidatePeerResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_route.ValidatePeerResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ValidatePeerResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut status__ = None;
                let mut message__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Status => {
                            if status__.is_some() {
                                return Err(serde::de::Error::duplicate_field("status"));
                            }
                            status__ = Some(map.next_value::<ValidatePeerStatus>()? as i32);
                        }
                        GeneratedField::Message => {
                            if message__.is_some() {
                                return Err(serde::de::Error::duplicate_field("message"));
                            }
                            message__ = Some(map.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(ValidatePeerResponse {
                    status: status__.unwrap_or_default(),
                    message: message__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_route.ValidatePeerResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ValidatePeerStatus {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::CreationUnknown => "CREATION_UNKNOWN",
            Self::Valid => "VALID",
            Self::Invalid => "INVALID",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for ValidatePeerStatus {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "CREATION_UNKNOWN",
            "VALID",
            "INVALID",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ValidatePeerStatus;

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
                    .and_then(ValidatePeerStatus::from_i32)
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
                    .and_then(ValidatePeerStatus::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "CREATION_UNKNOWN" => Ok(ValidatePeerStatus::CreationUnknown),
                    "VALID" => Ok(ValidatePeerStatus::Valid),
                    "INVALID" => Ok(ValidatePeerStatus::Invalid),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
