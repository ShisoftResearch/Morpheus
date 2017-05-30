use std::sync::Arc;
use neb::server::{ServerOptions, NebServer, ServerError};

pub struct MorpheusServer {
    neb_server: Arc<NebServer>
}

impl MorpheusServer {
    pub fn new(opts: &ServerOptions) -> Result<Arc<MorpheusServer>, ServerError> {
        let neb_server = NebServer::new(opts)?;
        Ok(Arc::new(MorpheusServer {
            neb_server: neb_server
        }))
    }
}