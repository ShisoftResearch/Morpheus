use bifrost::raft::state_machine::callback::server::{NotifyError, SMCallback};
use bifrost::raft::state_machine::StateMachineCtl;
use bifrost::raft::RaftService;
use bifrost::*;
use bifrost_hasher::hash_str;
use std::collections::HashMap;
use std::sync::Arc;

use super::GraphSchema;

pub static DEFAULT_RAFT_PREFIX: &'static str = "MORPHEUS_SCHEMA_RAFT_SM";

pub struct GraphSchemasSM {
    map: HashMap<u32, GraphSchema>,
    callback: SMCallback,
    sm_id: u64,
}

raft_state_machine! {
    def qry get_all() -> Vec<(u32, GraphSchema)>;
    def qry get(id: u32) -> Option<GraphSchema>;
    def cmd new_schema(id: u32, schema: GraphSchema) -> Result<(), NotifyError>;
    def cmd del_schema(id: u32) -> Result<(), NotifyError>;
    def sub on_schema_added() -> (u32, GraphSchema);
    def sub on_schema_deleted() -> u32;
}

impl StateMachineCmds for GraphSchemasSM {
    fn get_all<'a>(&'a self,) ->  BoxFuture<Vec<(u32,GraphSchema)> > {
        future::ready(self.get_all_local()).boxed()
    }

    fn get<'a>(&'a self, id:u32) ->  BoxFuture<Option<GraphSchema> > {
        future::ready(self.get_local(id)).boxed()
    }

    fn new_schema<'a>(&'a mut self,id:u32,schema:GraphSchema) ->  BoxFuture<Result<(),NotifyError> > {
        self.map.insert(id, schema);
        async {
            self.callback
                .notify(commands::on_schema_added::new(), schema)
                .await?;
            Ok(())
        }.boxed()
    }

    fn del_schema<'a>(&'a mut self,id:u32) ->  BoxFuture<Result<(),NotifyError> > {
        self.map.remove(&id).unwrap();
        async move {
            self.callback
                .notify(commands::on_schema_deleted::new(), id)
                .await?;
            Ok(())
        }
        .boxed()
    }
}

impl StateMachineCtl for GraphSchemasSM {
    raft_sm_complete!();
    fn id(&self) -> u64 {
        self.sm_id
    }
    fn snapshot(&self) -> Option<Vec<u8>> {
        Some(utils::serde::serialize(&self.map.iter().collect::<Vec<_>>() ))
    }
    fn recover(&mut self, data: Vec<u8>) -> BoxFuture<()> {
        let schemas: Vec<(u32, GraphSchema)> = utils::serde::deserialize(&data).unwrap();
        for (k, v) in schemas {
            self.map.insert(k, v);
        }
        future::ready(()).boxed()
    }
}

impl GraphSchemasSM {
    pub async fn new<'a>(sm_id: u64, raft_service: &Arc<RaftService>) -> Self {
        Self {
            callback: SMCallback::new(sm_id, raft_service.clone()).await,
            map: HashMap::with_capacity(64),
            sm_id,
        }
    }
    fn get_all_local(&self,) ->  Vec<(u32, GraphSchema)> {
        self.map.iter().map(|(k, v)| (*k, v.clone())).collect::<Vec<_>>()
    }

    fn get_local(&self, id:u32) ->  Option<GraphSchema> {
        self.map.get(&id).map(|s| s.clone())
    }
}