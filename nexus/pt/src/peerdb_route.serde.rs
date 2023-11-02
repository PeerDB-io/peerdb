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
        if self.create_catalog_entry {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_route.CreateQRepFlowRequest", len)?;
        if let Some(v) = self.qrep_config.as_ref() {
            struct_ser.serialize_field("qrepConfig", v)?;
        }
        if self.create_catalog_entry {
            struct_ser.serialize_field("createCatalogEntry", &self.create_catalog_entry)?;
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
            "create_catalog_entry",
            "createCatalogEntry",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            QrepConfig,
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
                            "qrepConfig" | "qrep_config" => Ok(GeneratedField::QrepConfig),
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
            type Value = CreateQRepFlowRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_route.CreateQRepFlowRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CreateQRepFlowRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut qrep_config__ = None;
                let mut create_catalog_entry__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::QrepConfig => {
                            if qrep_config__.is_some() {
                                return Err(serde::de::Error::duplicate_field("qrepConfig"));
                            }
                            qrep_config__ = map.next_value()?;
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
                Ok(CreateQRepFlowRequest {
                    qrep_config: qrep_config__,
                    create_catalog_entry: create_catalog_entry__.unwrap_or_default(),
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
impl serde::Serialize for DropPeerRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.peer_name.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_route.DropPeerRequest", len)?;
        if !self.peer_name.is_empty() {
            struct_ser.serialize_field("peerName", &self.peer_name)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for DropPeerRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "peer_name",
            "peerName",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            PeerName,
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
                            "peerName" | "peer_name" => Ok(GeneratedField::PeerName),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = DropPeerRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_route.DropPeerRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<DropPeerRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut peer_name__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::PeerName => {
                            if peer_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("peerName"));
                            }
                            peer_name__ = Some(map.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(DropPeerRequest {
                    peer_name: peer_name__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_route.DropPeerRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for DropPeerResponse {
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
        let mut struct_ser = serializer.serialize_struct("peerdb_route.DropPeerResponse", len)?;
        if self.ok {
            struct_ser.serialize_field("ok", &self.ok)?;
        }
        if !self.error_message.is_empty() {
            struct_ser.serialize_field("errorMessage", &self.error_message)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for DropPeerResponse {
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
            type Value = DropPeerResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_route.DropPeerResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<DropPeerResponse, V::Error>
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
                Ok(DropPeerResponse {
                    ok: ok__.unwrap_or_default(),
                    error_message: error_message__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_route.DropPeerResponse", FIELDS, GeneratedVisitor)
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
impl serde::Serialize for PeerSchemasResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.schemas.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_route.PeerSchemasResponse", len)?;
        if !self.schemas.is_empty() {
            struct_ser.serialize_field("schemas", &self.schemas)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PeerSchemasResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "schemas",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Schemas,
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
                            "schemas" => Ok(GeneratedField::Schemas),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PeerSchemasResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_route.PeerSchemasResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<PeerSchemasResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut schemas__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Schemas => {
                            if schemas__.is_some() {
                                return Err(serde::de::Error::duplicate_field("schemas"));
                            }
                            schemas__ = Some(map.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(PeerSchemasResponse {
                    schemas: schemas__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_route.PeerSchemasResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PeerSlotResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.slot_data.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_route.PeerSlotResponse", len)?;
        if !self.slot_data.is_empty() {
            struct_ser.serialize_field("slotData", &self.slot_data)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PeerSlotResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "slot_data",
            "slotData",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            SlotData,
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
                            "slotData" | "slot_data" => Ok(GeneratedField::SlotData),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PeerSlotResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_route.PeerSlotResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<PeerSlotResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut slot_data__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::SlotData => {
                            if slot_data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("slotData"));
                            }
                            slot_data__ = Some(map.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(PeerSlotResponse {
                    slot_data: slot_data__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_route.PeerSlotResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PeerStatResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.stat_data.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_route.PeerStatResponse", len)?;
        if !self.stat_data.is_empty() {
            struct_ser.serialize_field("statData", &self.stat_data)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PeerStatResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "stat_data",
            "statData",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            StatData,
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
                            "statData" | "stat_data" => Ok(GeneratedField::StatData),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PeerStatResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_route.PeerStatResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<PeerStatResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut stat_data__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::StatData => {
                            if stat_data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("statData"));
                            }
                            stat_data__ = Some(map.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(PeerStatResponse {
                    stat_data: stat_data__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_route.PeerStatResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PostgresPeerActivityInfoRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.peer_name.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_route.PostgresPeerActivityInfoRequest", len)?;
        if !self.peer_name.is_empty() {
            struct_ser.serialize_field("peerName", &self.peer_name)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PostgresPeerActivityInfoRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "peer_name",
            "peerName",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            PeerName,
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
                            "peerName" | "peer_name" => Ok(GeneratedField::PeerName),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PostgresPeerActivityInfoRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_route.PostgresPeerActivityInfoRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<PostgresPeerActivityInfoRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut peer_name__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::PeerName => {
                            if peer_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("peerName"));
                            }
                            peer_name__ = Some(map.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(PostgresPeerActivityInfoRequest {
                    peer_name: peer_name__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_route.PostgresPeerActivityInfoRequest", FIELDS, GeneratedVisitor)
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
impl serde::Serialize for SchemaTablesRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.peer_name.is_empty() {
            len += 1;
        }
        if !self.schema_name.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_route.SchemaTablesRequest", len)?;
        if !self.peer_name.is_empty() {
            struct_ser.serialize_field("peerName", &self.peer_name)?;
        }
        if !self.schema_name.is_empty() {
            struct_ser.serialize_field("schemaName", &self.schema_name)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SchemaTablesRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "peer_name",
            "peerName",
            "schema_name",
            "schemaName",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            PeerName,
            SchemaName,
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
                            "peerName" | "peer_name" => Ok(GeneratedField::PeerName),
                            "schemaName" | "schema_name" => Ok(GeneratedField::SchemaName),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SchemaTablesRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_route.SchemaTablesRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<SchemaTablesRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut peer_name__ = None;
                let mut schema_name__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::PeerName => {
                            if peer_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("peerName"));
                            }
                            peer_name__ = Some(map.next_value()?);
                        }
                        GeneratedField::SchemaName => {
                            if schema_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("schemaName"));
                            }
                            schema_name__ = Some(map.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(SchemaTablesRequest {
                    peer_name: peer_name__.unwrap_or_default(),
                    schema_name: schema_name__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_route.SchemaTablesRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SchemaTablesResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.tables.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_route.SchemaTablesResponse", len)?;
        if !self.tables.is_empty() {
            struct_ser.serialize_field("tables", &self.tables)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SchemaTablesResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "tables",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Tables,
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
                            "tables" => Ok(GeneratedField::Tables),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SchemaTablesResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_route.SchemaTablesResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<SchemaTablesResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut tables__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Tables => {
                            if tables__.is_some() {
                                return Err(serde::de::Error::duplicate_field("tables"));
                            }
                            tables__ = Some(map.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(SchemaTablesResponse {
                    tables: tables__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_route.SchemaTablesResponse", FIELDS, GeneratedVisitor)
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
impl serde::Serialize for SlotInfo {
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
        if !self.redo_l_sn.is_empty() {
            len += 1;
        }
        if !self.restart_l_sn.is_empty() {
            len += 1;
        }
        if self.active {
            len += 1;
        }
        if self.lag_in_mb != 0. {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_route.SlotInfo", len)?;
        if !self.slot_name.is_empty() {
            struct_ser.serialize_field("slotName", &self.slot_name)?;
        }
        if !self.redo_l_sn.is_empty() {
            struct_ser.serialize_field("redoLSN", &self.redo_l_sn)?;
        }
        if !self.restart_l_sn.is_empty() {
            struct_ser.serialize_field("restartLSN", &self.restart_l_sn)?;
        }
        if self.active {
            struct_ser.serialize_field("active", &self.active)?;
        }
        if self.lag_in_mb != 0. {
            struct_ser.serialize_field("lagInMb", &self.lag_in_mb)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SlotInfo {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "slot_name",
            "slotName",
            "redo_lSN",
            "redoLSN",
            "restart_lSN",
            "restartLSN",
            "active",
            "lag_in_mb",
            "lagInMb",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            SlotName,
            RedoLSn,
            RestartLSn,
            Active,
            LagInMb,
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
                            "redoLSN" | "redo_lSN" => Ok(GeneratedField::RedoLSn),
                            "restartLSN" | "restart_lSN" => Ok(GeneratedField::RestartLSn),
                            "active" => Ok(GeneratedField::Active),
                            "lagInMb" | "lag_in_mb" => Ok(GeneratedField::LagInMb),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SlotInfo;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_route.SlotInfo")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<SlotInfo, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut slot_name__ = None;
                let mut redo_l_sn__ = None;
                let mut restart_l_sn__ = None;
                let mut active__ = None;
                let mut lag_in_mb__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::SlotName => {
                            if slot_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("slotName"));
                            }
                            slot_name__ = Some(map.next_value()?);
                        }
                        GeneratedField::RedoLSn => {
                            if redo_l_sn__.is_some() {
                                return Err(serde::de::Error::duplicate_field("redoLSN"));
                            }
                            redo_l_sn__ = Some(map.next_value()?);
                        }
                        GeneratedField::RestartLSn => {
                            if restart_l_sn__.is_some() {
                                return Err(serde::de::Error::duplicate_field("restartLSN"));
                            }
                            restart_l_sn__ = Some(map.next_value()?);
                        }
                        GeneratedField::Active => {
                            if active__.is_some() {
                                return Err(serde::de::Error::duplicate_field("active"));
                            }
                            active__ = Some(map.next_value()?);
                        }
                        GeneratedField::LagInMb => {
                            if lag_in_mb__.is_some() {
                                return Err(serde::de::Error::duplicate_field("lagInMb"));
                            }
                            lag_in_mb__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(SlotInfo {
                    slot_name: slot_name__.unwrap_or_default(),
                    redo_l_sn: redo_l_sn__.unwrap_or_default(),
                    restart_l_sn: restart_l_sn__.unwrap_or_default(),
                    active: active__.unwrap_or_default(),
                    lag_in_mb: lag_in_mb__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_route.SlotInfo", FIELDS, GeneratedVisitor)
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
impl serde::Serialize for StatInfo {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.pid != 0 {
            len += 1;
        }
        if !self.wait_event.is_empty() {
            len += 1;
        }
        if !self.wait_event_type.is_empty() {
            len += 1;
        }
        if !self.query_start.is_empty() {
            len += 1;
        }
        if !self.query.is_empty() {
            len += 1;
        }
        if self.duration != 0. {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_route.StatInfo", len)?;
        if self.pid != 0 {
            struct_ser.serialize_field("pid", ToString::to_string(&self.pid).as_str())?;
        }
        if !self.wait_event.is_empty() {
            struct_ser.serialize_field("waitEvent", &self.wait_event)?;
        }
        if !self.wait_event_type.is_empty() {
            struct_ser.serialize_field("waitEventType", &self.wait_event_type)?;
        }
        if !self.query_start.is_empty() {
            struct_ser.serialize_field("queryStart", &self.query_start)?;
        }
        if !self.query.is_empty() {
            struct_ser.serialize_field("query", &self.query)?;
        }
        if self.duration != 0. {
            struct_ser.serialize_field("duration", &self.duration)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for StatInfo {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "pid",
            "wait_event",
            "waitEvent",
            "wait_event_type",
            "waitEventType",
            "query_start",
            "queryStart",
            "query",
            "duration",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Pid,
            WaitEvent,
            WaitEventType,
            QueryStart,
            Query,
            Duration,
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
                            "pid" => Ok(GeneratedField::Pid),
                            "waitEvent" | "wait_event" => Ok(GeneratedField::WaitEvent),
                            "waitEventType" | "wait_event_type" => Ok(GeneratedField::WaitEventType),
                            "queryStart" | "query_start" => Ok(GeneratedField::QueryStart),
                            "query" => Ok(GeneratedField::Query),
                            "duration" => Ok(GeneratedField::Duration),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = StatInfo;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_route.StatInfo")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<StatInfo, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut pid__ = None;
                let mut wait_event__ = None;
                let mut wait_event_type__ = None;
                let mut query_start__ = None;
                let mut query__ = None;
                let mut duration__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Pid => {
                            if pid__.is_some() {
                                return Err(serde::de::Error::duplicate_field("pid"));
                            }
                            pid__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::WaitEvent => {
                            if wait_event__.is_some() {
                                return Err(serde::de::Error::duplicate_field("waitEvent"));
                            }
                            wait_event__ = Some(map.next_value()?);
                        }
                        GeneratedField::WaitEventType => {
                            if wait_event_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("waitEventType"));
                            }
                            wait_event_type__ = Some(map.next_value()?);
                        }
                        GeneratedField::QueryStart => {
                            if query_start__.is_some() {
                                return Err(serde::de::Error::duplicate_field("queryStart"));
                            }
                            query_start__ = Some(map.next_value()?);
                        }
                        GeneratedField::Query => {
                            if query__.is_some() {
                                return Err(serde::de::Error::duplicate_field("query"));
                            }
                            query__ = Some(map.next_value()?);
                        }
                        GeneratedField::Duration => {
                            if duration__.is_some() {
                                return Err(serde::de::Error::duplicate_field("duration"));
                            }
                            duration__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(StatInfo {
                    pid: pid__.unwrap_or_default(),
                    wait_event: wait_event__.unwrap_or_default(),
                    wait_event_type: wait_event_type__.unwrap_or_default(),
                    query_start: query_start__.unwrap_or_default(),
                    query: query__.unwrap_or_default(),
                    duration: duration__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_route.StatInfo", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for TableColumnsRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.peer_name.is_empty() {
            len += 1;
        }
        if !self.schema_name.is_empty() {
            len += 1;
        }
        if !self.table_name.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_route.TableColumnsRequest", len)?;
        if !self.peer_name.is_empty() {
            struct_ser.serialize_field("peerName", &self.peer_name)?;
        }
        if !self.schema_name.is_empty() {
            struct_ser.serialize_field("schemaName", &self.schema_name)?;
        }
        if !self.table_name.is_empty() {
            struct_ser.serialize_field("tableName", &self.table_name)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for TableColumnsRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "peer_name",
            "peerName",
            "schema_name",
            "schemaName",
            "table_name",
            "tableName",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            PeerName,
            SchemaName,
            TableName,
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
                            "peerName" | "peer_name" => Ok(GeneratedField::PeerName),
                            "schemaName" | "schema_name" => Ok(GeneratedField::SchemaName),
                            "tableName" | "table_name" => Ok(GeneratedField::TableName),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = TableColumnsRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_route.TableColumnsRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<TableColumnsRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut peer_name__ = None;
                let mut schema_name__ = None;
                let mut table_name__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::PeerName => {
                            if peer_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("peerName"));
                            }
                            peer_name__ = Some(map.next_value()?);
                        }
                        GeneratedField::SchemaName => {
                            if schema_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("schemaName"));
                            }
                            schema_name__ = Some(map.next_value()?);
                        }
                        GeneratedField::TableName => {
                            if table_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableName"));
                            }
                            table_name__ = Some(map.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(TableColumnsRequest {
                    peer_name: peer_name__.unwrap_or_default(),
                    schema_name: schema_name__.unwrap_or_default(),
                    table_name: table_name__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_route.TableColumnsRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for TableColumnsResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.columns.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_route.TableColumnsResponse", len)?;
        if !self.columns.is_empty() {
            struct_ser.serialize_field("columns", &self.columns)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for TableColumnsResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "columns",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
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
            type Value = TableColumnsResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_route.TableColumnsResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<TableColumnsResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut columns__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
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
                Ok(TableColumnsResponse {
                    columns: columns__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_route.TableColumnsResponse", FIELDS, GeneratedVisitor)
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
