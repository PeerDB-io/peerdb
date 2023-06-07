use std::{default::Default, sync::Arc};

use async_trait::async_trait;
use peerdb_parser::{NexusParsedStatement, NexusQueryParser};
use pgwire::{
    api::{
        auth::{
            md5pass::{hash_md5_password, MakeMd5PasswordAuthStartupHandler},
            AuthSource, DefaultServerParameterProvider, LoginInfo, Password,
        },
        portal::Portal,
        query::{ExtendedQueryHandler, SimpleQueryHandler, StatementOrPortal},
        results::{DescribeResponse, Response},
        store::MemPortalStore,
        ClientInfo, MakeHandler,
    },
    error::PgWireResult,
    tokio::process_socket,
};
use rand::Rng;
use tokio::net::TcpListener;

struct DummyAuthSource;

#[async_trait]
impl AuthSource for DummyAuthSource {
    async fn get_password(&self, login_info: &LoginInfo) -> PgWireResult<Password> {
        println!("login info: {:?}", login_info);

        // randomly generate a 4 byte salt
        let salt = rand::thread_rng().gen::<[u8; 4]>().to_vec();
        let password = "peerdb";

        let hash_password =
            hash_md5_password(login_info.user().as_ref().unwrap(), password, salt.as_ref());
        Ok(Password::new(Some(salt), hash_password.as_bytes().to_vec()))
    }
}

pub struct NexusBackend {
    portal_store: Arc<MemPortalStore<NexusParsedStatement>>,
    query_parser: Arc<NexusQueryParser>,
}

#[async_trait]
impl SimpleQueryHandler for NexusBackend {
    async fn do_query<'a, C>(&self, _client: &C, _query: &'a str) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        todo!("implement simple query handler for NexusBackend")
    }
}

#[async_trait]
impl ExtendedQueryHandler for NexusBackend {
    type Statement = NexusParsedStatement;
    type PortalStore = MemPortalStore<Self::Statement>;
    type QueryParser = NexusQueryParser;

    fn portal_store(&self) -> Arc<Self::PortalStore> {
        self.portal_store.clone()
    }

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.query_parser.clone()
    }

    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        _portal: &'a Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        todo!("implement extended query handler for NexusBackend")
    }

    async fn do_describe<C>(
        &self,
        _client: &mut C,
        _target: StatementOrPortal<'_, Self::Statement>,
    ) -> PgWireResult<DescribeResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        todo!("implement describe handler for NexusBackend")
    }
}

struct MakeNexusBackend;

impl MakeNexusBackend {
    fn new() -> Self {
        Self
    }
}

impl MakeHandler for MakeNexusBackend {
    type Handler = Arc<NexusBackend>;

    fn make(&self) -> Self::Handler {
        let backend = NexusBackend {
            portal_store: Arc::new(MemPortalStore::new()),
            query_parser: Arc::new(Default::default()),
        };
        Arc::new(backend)
    }
}

#[tokio::main]
pub async fn main() {
    let authenticator = Arc::new(MakeMd5PasswordAuthStartupHandler::new(
        Arc::new(DummyAuthSource),
        Arc::new(DefaultServerParameterProvider),
    ));
    let processor = Arc::new(MakeNexusBackend::new());

    let server_addr = "127.0.0.1:5532";
    let listener = TcpListener::bind(server_addr).await.unwrap();
    println!("Listening on {}", server_addr);

    loop {
        let (socket, _) = listener.accept().await.unwrap();
        let authenticator_ref = authenticator.make();
        let processor_ref = processor.make();
        tokio::spawn(async move {
            process_socket(
                socket,
                None,
                authenticator_ref,
                processor_ref.clone(),
                processor_ref,
            )
            .await
        });
    }
}
