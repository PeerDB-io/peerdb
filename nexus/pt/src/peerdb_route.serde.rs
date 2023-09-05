// @generated
impl serde::Serialize for CreatePeerFlowRequest {
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
        let mut struct_ser = serializer.serialize_struct("peerdb_route.CreatePeerFlowRequest", len)?;
        if let Some(v) = self.connection_configs.as_ref() {
            struct_ser.serialize_field("connectionConfigs", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CreatePeerFlowRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "connection_configs",
            "connectionConfigs",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ConnectionConfigs,
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
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CreatePeerFlowRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_route.CreatePeerFlowRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CreatePeerFlowRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut connection_configs__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::ConnectionConfigs => {
                            if connection_configs__.is_some() {
                                return Err(serde::de::Error::duplicate_field("connectionConfigs"));
                            }
                            connection_configs__ = map.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(CreatePeerFlowRequest {
                    connection_configs: connection_configs__,
                })
            }
        }
        deserializer.deserialize_struct("peerdb_route.CreatePeerFlowRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CreatePeerFlowResponse {
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
        let mut struct_ser = serializer.serialize_struct("peerdb_route.CreatePeerFlowResponse", len)?;
        if !self.worflow_id.is_empty() {
            struct_ser.serialize_field("worflowId", &self.worflow_id)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CreatePeerFlowResponse {
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
            type Value = CreatePeerFlowResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_route.CreatePeerFlowResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CreatePeerFlowResponse, V::Error>
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
                Ok(CreatePeerFlowResponse {
                    worflow_id: worflow_id__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_route.CreatePeerFlowResponse", FIELDS, GeneratedVisitor)
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
impl serde::Serialize for ListPeersRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("peerdb_route.ListPeersRequest", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ListPeersRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
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
                            Ok(GeneratedField::__SkipField__)
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ListPeersRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_route.ListPeersRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ListPeersRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map.next_key::<GeneratedField>()?.is_some() {
                    let _ = map.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(ListPeersRequest {
                })
            }
        }
        deserializer.deserialize_struct("peerdb_route.ListPeersRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ListPeersResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.peers.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_route.ListPeersResponse", len)?;
        if !self.peers.is_empty() {
            struct_ser.serialize_field("peers", &self.peers)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ListPeersResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "peers",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Peers,
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
                            "peers" => Ok(GeneratedField::Peers),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ListPeersResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_route.ListPeersResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ListPeersResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut peers__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Peers => {
                            if peers__.is_some() {
                                return Err(serde::de::Error::duplicate_field("peers"));
                            }
                            peers__ = Some(map.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(ListPeersResponse {
                    peers: peers__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_route.ListPeersResponse", FIELDS, GeneratedVisitor)
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
