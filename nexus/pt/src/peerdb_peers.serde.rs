// @generated
impl serde::Serialize for BigqueryConfig {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.auth_type.is_empty() {
            len += 1;
        }
        if !self.project_id.is_empty() {
            len += 1;
        }
        if !self.private_key_id.is_empty() {
            len += 1;
        }
        if !self.private_key.is_empty() {
            len += 1;
        }
        if !self.client_email.is_empty() {
            len += 1;
        }
        if !self.client_id.is_empty() {
            len += 1;
        }
        if !self.auth_uri.is_empty() {
            len += 1;
        }
        if !self.token_uri.is_empty() {
            len += 1;
        }
        if !self.auth_provider_x509_cert_url.is_empty() {
            len += 1;
        }
        if !self.client_x509_cert_url.is_empty() {
            len += 1;
        }
        if !self.dataset_id.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_peers.BigqueryConfig", len)?;
        if !self.auth_type.is_empty() {
            struct_ser.serialize_field("authType", &self.auth_type)?;
        }
        if !self.project_id.is_empty() {
            struct_ser.serialize_field("projectId", &self.project_id)?;
        }
        if !self.private_key_id.is_empty() {
            struct_ser.serialize_field("privateKeyId", &self.private_key_id)?;
        }
        if !self.private_key.is_empty() {
            struct_ser.serialize_field("privateKey", &self.private_key)?;
        }
        if !self.client_email.is_empty() {
            struct_ser.serialize_field("clientEmail", &self.client_email)?;
        }
        if !self.client_id.is_empty() {
            struct_ser.serialize_field("clientId", &self.client_id)?;
        }
        if !self.auth_uri.is_empty() {
            struct_ser.serialize_field("authUri", &self.auth_uri)?;
        }
        if !self.token_uri.is_empty() {
            struct_ser.serialize_field("tokenUri", &self.token_uri)?;
        }
        if !self.auth_provider_x509_cert_url.is_empty() {
            struct_ser.serialize_field("authProviderX509CertUrl", &self.auth_provider_x509_cert_url)?;
        }
        if !self.client_x509_cert_url.is_empty() {
            struct_ser.serialize_field("clientX509CertUrl", &self.client_x509_cert_url)?;
        }
        if !self.dataset_id.is_empty() {
            struct_ser.serialize_field("datasetId", &self.dataset_id)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for BigqueryConfig {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "auth_type",
            "authType",
            "project_id",
            "projectId",
            "private_key_id",
            "privateKeyId",
            "private_key",
            "privateKey",
            "client_email",
            "clientEmail",
            "client_id",
            "clientId",
            "auth_uri",
            "authUri",
            "token_uri",
            "tokenUri",
            "auth_provider_x509_cert_url",
            "authProviderX509CertUrl",
            "client_x509_cert_url",
            "clientX509CertUrl",
            "dataset_id",
            "datasetId",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            AuthType,
            ProjectId,
            PrivateKeyId,
            PrivateKey,
            ClientEmail,
            ClientId,
            AuthUri,
            TokenUri,
            AuthProviderX509CertUrl,
            ClientX509CertUrl,
            DatasetId,
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
                            "authType" | "auth_type" => Ok(GeneratedField::AuthType),
                            "projectId" | "project_id" => Ok(GeneratedField::ProjectId),
                            "privateKeyId" | "private_key_id" => Ok(GeneratedField::PrivateKeyId),
                            "privateKey" | "private_key" => Ok(GeneratedField::PrivateKey),
                            "clientEmail" | "client_email" => Ok(GeneratedField::ClientEmail),
                            "clientId" | "client_id" => Ok(GeneratedField::ClientId),
                            "authUri" | "auth_uri" => Ok(GeneratedField::AuthUri),
                            "tokenUri" | "token_uri" => Ok(GeneratedField::TokenUri),
                            "authProviderX509CertUrl" | "auth_provider_x509_cert_url" => Ok(GeneratedField::AuthProviderX509CertUrl),
                            "clientX509CertUrl" | "client_x509_cert_url" => Ok(GeneratedField::ClientX509CertUrl),
                            "datasetId" | "dataset_id" => Ok(GeneratedField::DatasetId),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = BigqueryConfig;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_peers.BigqueryConfig")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<BigqueryConfig, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut auth_type__ = None;
                let mut project_id__ = None;
                let mut private_key_id__ = None;
                let mut private_key__ = None;
                let mut client_email__ = None;
                let mut client_id__ = None;
                let mut auth_uri__ = None;
                let mut token_uri__ = None;
                let mut auth_provider_x509_cert_url__ = None;
                let mut client_x509_cert_url__ = None;
                let mut dataset_id__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::AuthType => {
                            if auth_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("authType"));
                            }
                            auth_type__ = Some(map.next_value()?);
                        }
                        GeneratedField::ProjectId => {
                            if project_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("projectId"));
                            }
                            project_id__ = Some(map.next_value()?);
                        }
                        GeneratedField::PrivateKeyId => {
                            if private_key_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("privateKeyId"));
                            }
                            private_key_id__ = Some(map.next_value()?);
                        }
                        GeneratedField::PrivateKey => {
                            if private_key__.is_some() {
                                return Err(serde::de::Error::duplicate_field("privateKey"));
                            }
                            private_key__ = Some(map.next_value()?);
                        }
                        GeneratedField::ClientEmail => {
                            if client_email__.is_some() {
                                return Err(serde::de::Error::duplicate_field("clientEmail"));
                            }
                            client_email__ = Some(map.next_value()?);
                        }
                        GeneratedField::ClientId => {
                            if client_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("clientId"));
                            }
                            client_id__ = Some(map.next_value()?);
                        }
                        GeneratedField::AuthUri => {
                            if auth_uri__.is_some() {
                                return Err(serde::de::Error::duplicate_field("authUri"));
                            }
                            auth_uri__ = Some(map.next_value()?);
                        }
                        GeneratedField::TokenUri => {
                            if token_uri__.is_some() {
                                return Err(serde::de::Error::duplicate_field("tokenUri"));
                            }
                            token_uri__ = Some(map.next_value()?);
                        }
                        GeneratedField::AuthProviderX509CertUrl => {
                            if auth_provider_x509_cert_url__.is_some() {
                                return Err(serde::de::Error::duplicate_field("authProviderX509CertUrl"));
                            }
                            auth_provider_x509_cert_url__ = Some(map.next_value()?);
                        }
                        GeneratedField::ClientX509CertUrl => {
                            if client_x509_cert_url__.is_some() {
                                return Err(serde::de::Error::duplicate_field("clientX509CertUrl"));
                            }
                            client_x509_cert_url__ = Some(map.next_value()?);
                        }
                        GeneratedField::DatasetId => {
                            if dataset_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("datasetId"));
                            }
                            dataset_id__ = Some(map.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(BigqueryConfig {
                    auth_type: auth_type__.unwrap_or_default(),
                    project_id: project_id__.unwrap_or_default(),
                    private_key_id: private_key_id__.unwrap_or_default(),
                    private_key: private_key__.unwrap_or_default(),
                    client_email: client_email__.unwrap_or_default(),
                    client_id: client_id__.unwrap_or_default(),
                    auth_uri: auth_uri__.unwrap_or_default(),
                    token_uri: token_uri__.unwrap_or_default(),
                    auth_provider_x509_cert_url: auth_provider_x509_cert_url__.unwrap_or_default(),
                    client_x509_cert_url: client_x509_cert_url__.unwrap_or_default(),
                    dataset_id: dataset_id__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_peers.BigqueryConfig", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for DbType {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Bigquery => "BIGQUERY",
            Self::Snowflake => "SNOWFLAKE",
            Self::Mongo => "MONGO",
            Self::Postgres => "POSTGRES",
            Self::Eventhub => "EVENTHUB",
            Self::S3 => "S3",
            Self::Sqlserver => "SQLSERVER",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for DbType {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "BIGQUERY",
            "SNOWFLAKE",
            "MONGO",
            "POSTGRES",
            "EVENTHUB",
            "S3",
            "SQLSERVER",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = DbType;

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
                    .and_then(DbType::from_i32)
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
                    .and_then(DbType::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "BIGQUERY" => Ok(DbType::Bigquery),
                    "SNOWFLAKE" => Ok(DbType::Snowflake),
                    "MONGO" => Ok(DbType::Mongo),
                    "POSTGRES" => Ok(DbType::Postgres),
                    "EVENTHUB" => Ok(DbType::Eventhub),
                    "S3" => Ok(DbType::S3),
                    "SQLSERVER" => Ok(DbType::Sqlserver),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for EventHubConfig {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.namespace.is_empty() {
            len += 1;
        }
        if !self.resource_group.is_empty() {
            len += 1;
        }
        if !self.location.is_empty() {
            len += 1;
        }
        if self.metadata_db.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_peers.EventHubConfig", len)?;
        if !self.namespace.is_empty() {
            struct_ser.serialize_field("namespace", &self.namespace)?;
        }
        if !self.resource_group.is_empty() {
            struct_ser.serialize_field("resourceGroup", &self.resource_group)?;
        }
        if !self.location.is_empty() {
            struct_ser.serialize_field("location", &self.location)?;
        }
        if let Some(v) = self.metadata_db.as_ref() {
            struct_ser.serialize_field("metadataDb", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for EventHubConfig {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "namespace",
            "resource_group",
            "resourceGroup",
            "location",
            "metadata_db",
            "metadataDb",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Namespace,
            ResourceGroup,
            Location,
            MetadataDb,
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
                            "namespace" => Ok(GeneratedField::Namespace),
                            "resourceGroup" | "resource_group" => Ok(GeneratedField::ResourceGroup),
                            "location" => Ok(GeneratedField::Location),
                            "metadataDb" | "metadata_db" => Ok(GeneratedField::MetadataDb),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = EventHubConfig;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_peers.EventHubConfig")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<EventHubConfig, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut namespace__ = None;
                let mut resource_group__ = None;
                let mut location__ = None;
                let mut metadata_db__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Namespace => {
                            if namespace__.is_some() {
                                return Err(serde::de::Error::duplicate_field("namespace"));
                            }
                            namespace__ = Some(map.next_value()?);
                        }
                        GeneratedField::ResourceGroup => {
                            if resource_group__.is_some() {
                                return Err(serde::de::Error::duplicate_field("resourceGroup"));
                            }
                            resource_group__ = Some(map.next_value()?);
                        }
                        GeneratedField::Location => {
                            if location__.is_some() {
                                return Err(serde::de::Error::duplicate_field("location"));
                            }
                            location__ = Some(map.next_value()?);
                        }
                        GeneratedField::MetadataDb => {
                            if metadata_db__.is_some() {
                                return Err(serde::de::Error::duplicate_field("metadataDb"));
                            }
                            metadata_db__ = map.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(EventHubConfig {
                    namespace: namespace__.unwrap_or_default(),
                    resource_group: resource_group__.unwrap_or_default(),
                    location: location__.unwrap_or_default(),
                    metadata_db: metadata_db__,
                })
            }
        }
        deserializer.deserialize_struct("peerdb_peers.EventHubConfig", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for MongoConfig {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.username.is_empty() {
            len += 1;
        }
        if !self.password.is_empty() {
            len += 1;
        }
        if !self.clusterurl.is_empty() {
            len += 1;
        }
        if self.clusterport != 0 {
            len += 1;
        }
        if !self.database.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_peers.MongoConfig", len)?;
        if !self.username.is_empty() {
            struct_ser.serialize_field("username", &self.username)?;
        }
        if !self.password.is_empty() {
            struct_ser.serialize_field("password", &self.password)?;
        }
        if !self.clusterurl.is_empty() {
            struct_ser.serialize_field("clusterurl", &self.clusterurl)?;
        }
        if self.clusterport != 0 {
            struct_ser.serialize_field("clusterport", &self.clusterport)?;
        }
        if !self.database.is_empty() {
            struct_ser.serialize_field("database", &self.database)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for MongoConfig {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "username",
            "password",
            "clusterurl",
            "clusterport",
            "database",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Username,
            Password,
            Clusterurl,
            Clusterport,
            Database,
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
                            "username" => Ok(GeneratedField::Username),
                            "password" => Ok(GeneratedField::Password),
                            "clusterurl" => Ok(GeneratedField::Clusterurl),
                            "clusterport" => Ok(GeneratedField::Clusterport),
                            "database" => Ok(GeneratedField::Database),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = MongoConfig;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_peers.MongoConfig")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<MongoConfig, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut username__ = None;
                let mut password__ = None;
                let mut clusterurl__ = None;
                let mut clusterport__ = None;
                let mut database__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Username => {
                            if username__.is_some() {
                                return Err(serde::de::Error::duplicate_field("username"));
                            }
                            username__ = Some(map.next_value()?);
                        }
                        GeneratedField::Password => {
                            if password__.is_some() {
                                return Err(serde::de::Error::duplicate_field("password"));
                            }
                            password__ = Some(map.next_value()?);
                        }
                        GeneratedField::Clusterurl => {
                            if clusterurl__.is_some() {
                                return Err(serde::de::Error::duplicate_field("clusterurl"));
                            }
                            clusterurl__ = Some(map.next_value()?);
                        }
                        GeneratedField::Clusterport => {
                            if clusterport__.is_some() {
                                return Err(serde::de::Error::duplicate_field("clusterport"));
                            }
                            clusterport__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Database => {
                            if database__.is_some() {
                                return Err(serde::de::Error::duplicate_field("database"));
                            }
                            database__ = Some(map.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(MongoConfig {
                    username: username__.unwrap_or_default(),
                    password: password__.unwrap_or_default(),
                    clusterurl: clusterurl__.unwrap_or_default(),
                    clusterport: clusterport__.unwrap_or_default(),
                    database: database__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_peers.MongoConfig", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Peer {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.name.is_empty() {
            len += 1;
        }
        if self.r#type != 0 {
            len += 1;
        }
        if self.config.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_peers.Peer", len)?;
        if !self.name.is_empty() {
            struct_ser.serialize_field("name", &self.name)?;
        }
        if self.r#type != 0 {
            let v = DbType::from_i32(self.r#type)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.r#type)))?;
            struct_ser.serialize_field("type", &v)?;
        }
        if let Some(v) = self.config.as_ref() {
            match v {
                peer::Config::SnowflakeConfig(v) => {
                    struct_ser.serialize_field("snowflakeConfig", v)?;
                }
                peer::Config::BigqueryConfig(v) => {
                    struct_ser.serialize_field("bigqueryConfig", v)?;
                }
                peer::Config::MongoConfig(v) => {
                    struct_ser.serialize_field("mongoConfig", v)?;
                }
                peer::Config::PostgresConfig(v) => {
                    struct_ser.serialize_field("postgresConfig", v)?;
                }
                peer::Config::EventhubConfig(v) => {
                    struct_ser.serialize_field("eventhubConfig", v)?;
                }
                peer::Config::S3Config(v) => {
                    struct_ser.serialize_field("s3Config", v)?;
                }
                peer::Config::SqlserverConfig(v) => {
                    struct_ser.serialize_field("sqlserverConfig", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Peer {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "name",
            "type",
            "snowflake_config",
            "snowflakeConfig",
            "bigquery_config",
            "bigqueryConfig",
            "mongo_config",
            "mongoConfig",
            "postgres_config",
            "postgresConfig",
            "eventhub_config",
            "eventhubConfig",
            "s3_config",
            "s3Config",
            "sqlserver_config",
            "sqlserverConfig",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Name,
            Type,
            SnowflakeConfig,
            BigqueryConfig,
            MongoConfig,
            PostgresConfig,
            EventhubConfig,
            S3Config,
            SqlserverConfig,
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
                            "name" => Ok(GeneratedField::Name),
                            "type" => Ok(GeneratedField::Type),
                            "snowflakeConfig" | "snowflake_config" => Ok(GeneratedField::SnowflakeConfig),
                            "bigqueryConfig" | "bigquery_config" => Ok(GeneratedField::BigqueryConfig),
                            "mongoConfig" | "mongo_config" => Ok(GeneratedField::MongoConfig),
                            "postgresConfig" | "postgres_config" => Ok(GeneratedField::PostgresConfig),
                            "eventhubConfig" | "eventhub_config" => Ok(GeneratedField::EventhubConfig),
                            "s3Config" | "s3_config" => Ok(GeneratedField::S3Config),
                            "sqlserverConfig" | "sqlserver_config" => Ok(GeneratedField::SqlserverConfig),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Peer;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_peers.Peer")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<Peer, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut name__ = None;
                let mut r#type__ = None;
                let mut config__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = Some(map.next_value()?);
                        }
                        GeneratedField::Type => {
                            if r#type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("type"));
                            }
                            r#type__ = Some(map.next_value::<DbType>()? as i32);
                        }
                        GeneratedField::SnowflakeConfig => {
                            if config__.is_some() {
                                return Err(serde::de::Error::duplicate_field("snowflakeConfig"));
                            }
                            config__ = map.next_value::<::std::option::Option<_>>()?.map(peer::Config::SnowflakeConfig)
;
                        }
                        GeneratedField::BigqueryConfig => {
                            if config__.is_some() {
                                return Err(serde::de::Error::duplicate_field("bigqueryConfig"));
                            }
                            config__ = map.next_value::<::std::option::Option<_>>()?.map(peer::Config::BigqueryConfig)
;
                        }
                        GeneratedField::MongoConfig => {
                            if config__.is_some() {
                                return Err(serde::de::Error::duplicate_field("mongoConfig"));
                            }
                            config__ = map.next_value::<::std::option::Option<_>>()?.map(peer::Config::MongoConfig)
;
                        }
                        GeneratedField::PostgresConfig => {
                            if config__.is_some() {
                                return Err(serde::de::Error::duplicate_field("postgresConfig"));
                            }
                            config__ = map.next_value::<::std::option::Option<_>>()?.map(peer::Config::PostgresConfig)
;
                        }
                        GeneratedField::EventhubConfig => {
                            if config__.is_some() {
                                return Err(serde::de::Error::duplicate_field("eventhubConfig"));
                            }
                            config__ = map.next_value::<::std::option::Option<_>>()?.map(peer::Config::EventhubConfig)
;
                        }
                        GeneratedField::S3Config => {
                            if config__.is_some() {
                                return Err(serde::de::Error::duplicate_field("s3Config"));
                            }
                            config__ = map.next_value::<::std::option::Option<_>>()?.map(peer::Config::S3Config)
;
                        }
                        GeneratedField::SqlserverConfig => {
                            if config__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sqlserverConfig"));
                            }
                            config__ = map.next_value::<::std::option::Option<_>>()?.map(peer::Config::SqlserverConfig)
;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(Peer {
                    name: name__.unwrap_or_default(),
                    r#type: r#type__.unwrap_or_default(),
                    config: config__,
                })
            }
        }
        deserializer.deserialize_struct("peerdb_peers.Peer", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PostgresConfig {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.host.is_empty() {
            len += 1;
        }
        if self.port != 0 {
            len += 1;
        }
        if !self.user.is_empty() {
            len += 1;
        }
        if !self.password.is_empty() {
            len += 1;
        }
        if !self.database.is_empty() {
            len += 1;
        }
        if !self.transaction_snapshot.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_peers.PostgresConfig", len)?;
        if !self.host.is_empty() {
            struct_ser.serialize_field("host", &self.host)?;
        }
        if self.port != 0 {
            struct_ser.serialize_field("port", &self.port)?;
        }
        if !self.user.is_empty() {
            struct_ser.serialize_field("user", &self.user)?;
        }
        if !self.password.is_empty() {
            struct_ser.serialize_field("password", &self.password)?;
        }
        if !self.database.is_empty() {
            struct_ser.serialize_field("database", &self.database)?;
        }
        if !self.transaction_snapshot.is_empty() {
            struct_ser.serialize_field("transactionSnapshot", &self.transaction_snapshot)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PostgresConfig {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "host",
            "port",
            "user",
            "password",
            "database",
            "transaction_snapshot",
            "transactionSnapshot",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Host,
            Port,
            User,
            Password,
            Database,
            TransactionSnapshot,
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
                            "host" => Ok(GeneratedField::Host),
                            "port" => Ok(GeneratedField::Port),
                            "user" => Ok(GeneratedField::User),
                            "password" => Ok(GeneratedField::Password),
                            "database" => Ok(GeneratedField::Database),
                            "transactionSnapshot" | "transaction_snapshot" => Ok(GeneratedField::TransactionSnapshot),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PostgresConfig;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_peers.PostgresConfig")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<PostgresConfig, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut host__ = None;
                let mut port__ = None;
                let mut user__ = None;
                let mut password__ = None;
                let mut database__ = None;
                let mut transaction_snapshot__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Host => {
                            if host__.is_some() {
                                return Err(serde::de::Error::duplicate_field("host"));
                            }
                            host__ = Some(map.next_value()?);
                        }
                        GeneratedField::Port => {
                            if port__.is_some() {
                                return Err(serde::de::Error::duplicate_field("port"));
                            }
                            port__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::User => {
                            if user__.is_some() {
                                return Err(serde::de::Error::duplicate_field("user"));
                            }
                            user__ = Some(map.next_value()?);
                        }
                        GeneratedField::Password => {
                            if password__.is_some() {
                                return Err(serde::de::Error::duplicate_field("password"));
                            }
                            password__ = Some(map.next_value()?);
                        }
                        GeneratedField::Database => {
                            if database__.is_some() {
                                return Err(serde::de::Error::duplicate_field("database"));
                            }
                            database__ = Some(map.next_value()?);
                        }
                        GeneratedField::TransactionSnapshot => {
                            if transaction_snapshot__.is_some() {
                                return Err(serde::de::Error::duplicate_field("transactionSnapshot"));
                            }
                            transaction_snapshot__ = Some(map.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(PostgresConfig {
                    host: host__.unwrap_or_default(),
                    port: port__.unwrap_or_default(),
                    user: user__.unwrap_or_default(),
                    password: password__.unwrap_or_default(),
                    database: database__.unwrap_or_default(),
                    transaction_snapshot: transaction_snapshot__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_peers.PostgresConfig", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for S3Config {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.url.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_peers.S3Config", len)?;
        if !self.url.is_empty() {
            struct_ser.serialize_field("url", &self.url)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for S3Config {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "url",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Url,
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
                            "url" => Ok(GeneratedField::Url),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = S3Config;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_peers.S3Config")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<S3Config, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut url__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Url => {
                            if url__.is_some() {
                                return Err(serde::de::Error::duplicate_field("url"));
                            }
                            url__ = Some(map.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(S3Config {
                    url: url__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_peers.S3Config", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SnowflakeConfig {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.account_id.is_empty() {
            len += 1;
        }
        if !self.username.is_empty() {
            len += 1;
        }
        if !self.private_key.is_empty() {
            len += 1;
        }
        if !self.database.is_empty() {
            len += 1;
        }
        if !self.warehouse.is_empty() {
            len += 1;
        }
        if !self.role.is_empty() {
            len += 1;
        }
        if self.query_timeout != 0 {
            len += 1;
        }
        if !self.s3_integration.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_peers.SnowflakeConfig", len)?;
        if !self.account_id.is_empty() {
            struct_ser.serialize_field("accountId", &self.account_id)?;
        }
        if !self.username.is_empty() {
            struct_ser.serialize_field("username", &self.username)?;
        }
        if !self.private_key.is_empty() {
            struct_ser.serialize_field("privateKey", &self.private_key)?;
        }
        if !self.database.is_empty() {
            struct_ser.serialize_field("database", &self.database)?;
        }
        if !self.warehouse.is_empty() {
            struct_ser.serialize_field("warehouse", &self.warehouse)?;
        }
        if !self.role.is_empty() {
            struct_ser.serialize_field("role", &self.role)?;
        }
        if self.query_timeout != 0 {
            struct_ser.serialize_field("queryTimeout", ToString::to_string(&self.query_timeout).as_str())?;
        }
        if !self.s3_integration.is_empty() {
            struct_ser.serialize_field("s3Integration", &self.s3_integration)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SnowflakeConfig {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "account_id",
            "accountId",
            "username",
            "private_key",
            "privateKey",
            "database",
            "warehouse",
            "role",
            "query_timeout",
            "queryTimeout",
            "s3_integration",
            "s3Integration",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            AccountId,
            Username,
            PrivateKey,
            Database,
            Warehouse,
            Role,
            QueryTimeout,
            S3Integration,
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
                            "accountId" | "account_id" => Ok(GeneratedField::AccountId),
                            "username" => Ok(GeneratedField::Username),
                            "privateKey" | "private_key" => Ok(GeneratedField::PrivateKey),
                            "database" => Ok(GeneratedField::Database),
                            "warehouse" => Ok(GeneratedField::Warehouse),
                            "role" => Ok(GeneratedField::Role),
                            "queryTimeout" | "query_timeout" => Ok(GeneratedField::QueryTimeout),
                            "s3Integration" | "s3_integration" => Ok(GeneratedField::S3Integration),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SnowflakeConfig;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_peers.SnowflakeConfig")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<SnowflakeConfig, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut account_id__ = None;
                let mut username__ = None;
                let mut private_key__ = None;
                let mut database__ = None;
                let mut warehouse__ = None;
                let mut role__ = None;
                let mut query_timeout__ = None;
                let mut s3_integration__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::AccountId => {
                            if account_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("accountId"));
                            }
                            account_id__ = Some(map.next_value()?);
                        }
                        GeneratedField::Username => {
                            if username__.is_some() {
                                return Err(serde::de::Error::duplicate_field("username"));
                            }
                            username__ = Some(map.next_value()?);
                        }
                        GeneratedField::PrivateKey => {
                            if private_key__.is_some() {
                                return Err(serde::de::Error::duplicate_field("privateKey"));
                            }
                            private_key__ = Some(map.next_value()?);
                        }
                        GeneratedField::Database => {
                            if database__.is_some() {
                                return Err(serde::de::Error::duplicate_field("database"));
                            }
                            database__ = Some(map.next_value()?);
                        }
                        GeneratedField::Warehouse => {
                            if warehouse__.is_some() {
                                return Err(serde::de::Error::duplicate_field("warehouse"));
                            }
                            warehouse__ = Some(map.next_value()?);
                        }
                        GeneratedField::Role => {
                            if role__.is_some() {
                                return Err(serde::de::Error::duplicate_field("role"));
                            }
                            role__ = Some(map.next_value()?);
                        }
                        GeneratedField::QueryTimeout => {
                            if query_timeout__.is_some() {
                                return Err(serde::de::Error::duplicate_field("queryTimeout"));
                            }
                            query_timeout__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::S3Integration => {
                            if s3_integration__.is_some() {
                                return Err(serde::de::Error::duplicate_field("s3Integration"));
                            }
                            s3_integration__ = Some(map.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(SnowflakeConfig {
                    account_id: account_id__.unwrap_or_default(),
                    username: username__.unwrap_or_default(),
                    private_key: private_key__.unwrap_or_default(),
                    database: database__.unwrap_or_default(),
                    warehouse: warehouse__.unwrap_or_default(),
                    role: role__.unwrap_or_default(),
                    query_timeout: query_timeout__.unwrap_or_default(),
                    s3_integration: s3_integration__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_peers.SnowflakeConfig", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SqlServerConfig {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.server.is_empty() {
            len += 1;
        }
        if self.port != 0 {
            len += 1;
        }
        if !self.user.is_empty() {
            len += 1;
        }
        if !self.password.is_empty() {
            len += 1;
        }
        if !self.database.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("peerdb_peers.SqlServerConfig", len)?;
        if !self.server.is_empty() {
            struct_ser.serialize_field("server", &self.server)?;
        }
        if self.port != 0 {
            struct_ser.serialize_field("port", &self.port)?;
        }
        if !self.user.is_empty() {
            struct_ser.serialize_field("user", &self.user)?;
        }
        if !self.password.is_empty() {
            struct_ser.serialize_field("password", &self.password)?;
        }
        if !self.database.is_empty() {
            struct_ser.serialize_field("database", &self.database)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SqlServerConfig {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "server",
            "port",
            "user",
            "password",
            "database",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Server,
            Port,
            User,
            Password,
            Database,
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
                            "server" => Ok(GeneratedField::Server),
                            "port" => Ok(GeneratedField::Port),
                            "user" => Ok(GeneratedField::User),
                            "password" => Ok(GeneratedField::Password),
                            "database" => Ok(GeneratedField::Database),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SqlServerConfig;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct peerdb_peers.SqlServerConfig")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<SqlServerConfig, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut server__ = None;
                let mut port__ = None;
                let mut user__ = None;
                let mut password__ = None;
                let mut database__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Server => {
                            if server__.is_some() {
                                return Err(serde::de::Error::duplicate_field("server"));
                            }
                            server__ = Some(map.next_value()?);
                        }
                        GeneratedField::Port => {
                            if port__.is_some() {
                                return Err(serde::de::Error::duplicate_field("port"));
                            }
                            port__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::User => {
                            if user__.is_some() {
                                return Err(serde::de::Error::duplicate_field("user"));
                            }
                            user__ = Some(map.next_value()?);
                        }
                        GeneratedField::Password => {
                            if password__.is_some() {
                                return Err(serde::de::Error::duplicate_field("password"));
                            }
                            password__ = Some(map.next_value()?);
                        }
                        GeneratedField::Database => {
                            if database__.is_some() {
                                return Err(serde::de::Error::duplicate_field("database"));
                            }
                            database__ = Some(map.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(SqlServerConfig {
                    server: server__.unwrap_or_default(),
                    port: port__.unwrap_or_default(),
                    user: user__.unwrap_or_default(),
                    password: password__.unwrap_or_default(),
                    database: database__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("peerdb_peers.SqlServerConfig", FIELDS, GeneratedVisitor)
    }
}
