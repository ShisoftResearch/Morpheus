use bifrost::raft::state_machine::master::ExecError;
use bifrost::rpc::RPCError;
use neb::client::transaction::{Transaction, TxnError};
use neb::client::AsyncClient as NebClient;
use neb::dovahkiin::expr::SExpr;
use neb::dovahkiin::types::{ToValue, Value};
use neb::ram::cell::{Cell, OwnedCell, ReadError, SharedCell, WriteError};
use neb::ram::schema::{Field, Schema};
use neb::ram::types::{key_hash, Id};

use crate::graph::edge::bilateral::BilateralEdge;
use crate::graph::edge::{EdgeAttributes, EdgeError};
use crate::graph::vertex::{ToVertexId, Vertex};
use crate::query::{parse_optional_expr, Expr, Tester};
use crate::server::schema::{MorpheusSchema, SchemaContainer, SchemaError, GraphSchema, ToSchemaId};

use std::future::Future;

use dovahkiin::types::{Map, OwnedMap, OwnedValue};

use std::sync::Arc;

pub mod edge;
pub mod fields;
mod id_list;
pub mod vertex;

#[derive(Debug)]
pub enum NewVertexError {
    SchemaNotFound,
    SchemaNotVertex(GraphSchema),
    CannotGenerateCellByData,
    DataNotMap,
    RPCError(RPCError),
    WriteError(WriteError),
}

#[derive(Debug)]
pub enum ReadVertexError {
    RPCError(RPCError),
    ReadError(ReadError),
}

#[derive(Debug)]
pub enum LinkVerticesError {
    EdgeSchemaNotFound,
    SchemaNotEdge,
    BodyRequired,
    BodyShouldNotExisted,
    EdgeError(edge::EdgeError),
}

#[derive(Debug)]
pub enum NeighbourhoodError {
    EdgeError(edge::EdgeError),
    VertexNotFound(Id),
    CannotFindOppositeId(Id),
    FilterEvalError(String),
}

#[derive(Clone, Copy, Serialize, Deserialize)]
pub enum CellType {
    Vertex,
    Edge(edge::EdgeType),
}

#[derive(Clone, Copy)]
pub enum EdgeDirection {
    Inbound,
    Outbound,
    Undirected,
}

impl EdgeDirection {
    pub fn as_field(&self) -> u64 {
        match self {
            &EdgeDirection::Inbound => *fields::INBOUND_KEY_ID,
            &EdgeDirection::Outbound => *fields::OUTBOUND_KEY_ID,
            &EdgeDirection::Undirected => *fields::UNDIRECTED_KEY_ID,
        }
    }
}

fn vertex_to_cell_for_write(
    schemas: &Arc<SchemaContainer>,
    vertex: Vertex,
) -> Result<OwnedCell, NewVertexError> {
    let schema_id = vertex.schema();
    if let Some(stype) = schemas.schema_type(schema_id) {
        if stype != GraphSchema::Vertex {
            return Err(NewVertexError::SchemaNotVertex(stype));
        }
    } else {
        return Err(NewVertexError::SchemaNotFound);
    }
    let neb_schema = match schemas.get_neb_schema(schema_id) {
        Some(schema) => schema,
        None => return Err(NewVertexError::SchemaNotFound),
    };
    let mut data = {
        match vertex.cell.data {
            OwnedValue::Map(map) => map,
            _ => return Err(NewVertexError::DataNotMap),
        }
    };
    data.insert_key_id(*fields::INBOUND_KEY_ID, OwnedValue::Id(Id::unit_id()));
    data.insert_key_id(*fields::OUTBOUND_KEY_ID, OwnedValue::Id(Id::unit_id()));
    data.insert_key_id(*fields::UNDIRECTED_KEY_ID, OwnedValue::Id(Id::unit_id()));
    match OwnedCell::new(&neb_schema, OwnedValue::Map(data)) {
        Some(cell) => Ok(cell),
        None => return Err(NewVertexError::CannotGenerateCellByData),
    }
}

pub struct Graph {
    schemas: Arc<SchemaContainer>,
    neb_client: Arc<NebClient>,
}

impl Graph {
    pub async fn new(
        schemas: &Arc<SchemaContainer>,
        neb_client: &Arc<NebClient>,
    ) -> Result<Self, ExecError> {
        Self::check_base_schemas(&*schemas).await?;
        Ok(Self {
            schemas: schemas.clone(),
            neb_client: neb_client.clone(),
        })
    }

    async fn check_base_schema(
        schemas: &Arc<SchemaContainer>,
        schema_id: u32,
        schema_name: &'static str,
        fields: &'static Field,
    ) -> Result<(), ExecError> {
        match schemas.get_neb_schema(schema_id) {
            None => {
                schemas
                    .neb_client
                    .new_schema_with_id(Schema::new_with_id(
                        schema_id,
                        schema_name,
                        None,
                        fields.clone(),
                        false,
                        false,
                    ))
                    .await?;
            }
            _ => {}
        }
        Ok(())
    }

    async fn check_base_schemas(schemas: &Arc<SchemaContainer>) -> Result<(), ExecError> {
        Self::check_base_schema(
            schemas,
            id_list::ID_LIST_SCHEMA_ID,
            "_NEB_ID_LIST",
            &*id_list::ID_LINKED_LIST,
        )
        .await?;
        Self::check_base_schema(
            schemas,
            id_list::TYPE_LIST_SCHEMA_ID,
            "_NEB_TYPE_ID_LIST",
            &*id_list::ID_TYPE_LIST,
        )
        .await?;
        Ok(())
    }
    pub fn new_vertex_group(
        &self,
        mut schema: MorpheusSchema,
    ) -> impl Future<Output = Result<u32, SchemaError>> {
        schema.schema_type = GraphSchema::Vertex;
        self.schemas.new_schema(schema)
    }
    pub fn new_edge_group(
        &self,
        mut schema: MorpheusSchema,
        edge_attrs: edge::EdgeAttributes,
    ) -> impl Future<Output = Result<u32, SchemaError>> {
        schema.schema_type = GraphSchema::Edge(edge_attrs);
        self.schemas.new_schema(schema)
    }
    pub async fn new_vertex<S>(&self, schema: S, data: OwnedMap) -> Result<Vertex, NewVertexError>
    where
        S: ToSchemaId,
    {
        let vertex = Vertex::new(schema.to_id(&self.schemas), data);
        let mut cell = vertex_to_cell_for_write(&self.schemas, vertex)?;
        let header = match self.neb_client.write_cell(cell.clone()).await {
            Ok(Ok(header)) => header,
            Ok(Err(e)) => return Err(NewVertexError::WriteError(e)),
            Err(e) => return Err(NewVertexError::RPCError(e)),
        };
        cell.header = header;
        Ok(vertex::cell_to_vertex(cell))
    }
    pub async fn remove_vertex<V>(&self, vertex: V) -> Result<(), TxnError>
    where
        V: ToVertexId,
    {
        let id = vertex.to_id();
        self.graph_transaction(move |txn| async move {
            txn.remove_vertex(id)
                .await?
                .map_err(|_| TxnError::Aborted(None))
        })
        .await
    }
    pub async fn remove_vertex_by_key<K, S>(&self, schema: S, key: K) -> Result<(), TxnError>
    where
        K: ToValue,
        S: ToSchemaId,
    {
        let id = OwnedCell::encode_cell_key(schema.to_id(&self.schemas), &key.value());
        self.remove_vertex(id).await
    }
    pub fn update_vertex<'a, V, U>(&'a self, vertex: V, update: U) -> impl Future<Output = Result<(), TxnError>> + 'a
    where
        V: ToVertexId,
        U: Fn(Vertex) -> Option<Vertex> + Clone,
        U: 'a,
    {
        let id = vertex.to_id();
        self.neb_client
            .transaction(move |txn| {
                let update = update.clone();
                let txn = txn.clone();
                async move {
                    vertex::txn_update(&txn, id, update).await
                }
            })
    }
    pub fn update_vertex_by_key<K, U, S>(
        &self,
        schema: S,
        key: K,
        update: U,
    ) -> impl Future<Output = Result<(), TxnError>> + '_
    where
        K: ToValue,
        S: ToSchemaId,
        U: Fn(Vertex) -> Option<Vertex> + Clone + 'static,
    {
        let id = OwnedCell::encode_cell_key(schema.to_id(&self.schemas), &key.value());
        self.update_vertex(id, update)
    }

    pub async fn vertex_by<V>(
        &self,
        vertex: V,
    ) -> Result<Option<Vertex>, ReadVertexError>
    where
        V: ToVertexId,
    {
        match self.neb_client.read_cell(vertex.to_id()).await {
            Err(e) => Err(ReadVertexError::RPCError(e)),
            Ok(Err(ReadError::CellDoesNotExisted)) => Ok(None),
            Ok(Err(e)) => Err(ReadVertexError::ReadError(e)),
            Ok(Ok(cell)) => Ok(Some(vertex::cell_to_vertex(cell))),
        }
    }

    pub fn vertex_by_key<K, S>(
        &self,
        schema: S,
        key: K,
    ) -> impl Future<Output = Result<Option<Vertex>, ReadVertexError>> + '_
    where
        K: ToValue,
        S: ToSchemaId,
    {
        let id = OwnedCell::encode_cell_key(schema.to_id(&self.schemas), &key.value());
        self.vertex_by(id)
    }

    pub async fn graph_transaction<'a, TFN, TR, TRF>(&self, func: TFN) -> Result<TR, TxnError>
    where
        TFN: Fn(GraphTransaction) -> TRF + 'a,
        TR: 'a,
        TFN: 'a,
        TRF: Future<Output = Result<TR, TxnError>> + 'a,
    {
        let schemas = self.schemas.clone();
        let wrapper = move |neb_txn: Transaction| {
            func(GraphTransaction {
                neb_txn,
                schemas: schemas.clone(),
            })
        };
        self.neb_client.transaction(wrapper).await
    }
    pub async fn link<V, S>(
        &self,
        from: V,
        schema: S,
        to: V,
        body: &Option<OwnedMap>,
    ) -> Result<Result<edge::Edge, LinkVerticesError>, TxnError>
    where
        V: ToVertexId,
        S: ToSchemaId,
    {
        let from_id = from.to_id();
        let to_id = to.to_id();
        let schema_id = schema.to_id(&self.schemas);
        self.graph_transaction(
            move |txn| async move { txn.link(from_id, schema_id, to_id, body).await },
        )
        .await
    }
    pub async fn degree<V, S>(
        &self,
        vertex: V,
        schema: S,
        ed: EdgeDirection,
    ) -> Result<Result<usize, edge::EdgeError>, TxnError>
    where
        V: ToVertexId,
        S: ToSchemaId,
    {
        let vertex_id = vertex.to_id();
        let schema_id = schema.to_id(&self.schemas);
        self.graph_transaction(move |txn| async move { txn.degree(vertex_id, schema_id, ed).await })
            .await
    }
    pub async fn neighbourhoods<V, S, F>(
        &self,
        vertex: V,
        schema: S,
        ed: EdgeDirection,
        filter: &Option<F>,
    ) -> Result<Result<Vec<(Vertex, edge::Edge)>, NeighbourhoodError>, TxnError>
    where
        V: ToVertexId,
        S: ToSchemaId,
        F: Expr,
    {
        let vertex_id = vertex.to_id();
        let schema_id = schema.to_id(&self.schemas);
        match parse_optional_expr(filter) {
            Err(e) => Ok(Err(NeighbourhoodError::FilterEvalError(e))),
            Ok(filter_sexpr) => {
                self.graph_transaction(move |txn| {
                    let filter_sexpr = filter_sexpr.clone();
                    async move {
                        txn.neighbourhoods(vertex_id, schema_id, ed, &filter_sexpr)
                            .await
                    }
                })
                .await
            }
        }
    }
    pub async fn edges<V, S, F>(
        &self,
        vertex: V,
        schema: S,
        ed: EdgeDirection,
        filter: &Option<F>,
    ) -> Result<Result<Vec<edge::Edge>, EdgeError>, TxnError>
    where
        V: ToVertexId,
        S: ToSchemaId,
        F: Expr,
    {
        let vertex_id = vertex.to_id();
        let schema_id = schema.to_id(&self.schemas);
        match parse_optional_expr(filter) {
            Err(e) => Ok(Err(EdgeError::FilterEvalError(e))),
            Ok(filter) => {
                self.graph_transaction(move |txn| {
                    let filter = filter.clone();
                    async move { txn.edges(vertex_id, schema_id, ed, &filter).await }
                })
                .await
            }
        }
    }
}

pub struct GraphTransaction {
    pub neb_txn: Transaction,
    schemas: Arc<SchemaContainer>,
}

impl GraphTransaction {
    pub async fn new_vertex<S>(
        &self,
        schema: S,
        data: OwnedMap,
    ) -> Result<Result<Vertex, NewVertexError>, TxnError>
    where
        S: ToSchemaId,
    {
        let vertex = Vertex::new(schema.to_id(&self.schemas), data);
        let cell = match vertex_to_cell_for_write(&self.schemas, vertex) {
            Ok(cell) => cell,
            Err(e) => return Ok(Err(e)),
        };
        self.neb_txn.write(cell.clone()).await?;
        Ok(Ok(vertex::cell_to_vertex(cell)))
    }
    pub async fn remove_vertex<V>(
        &self,
        vertex: V,
    ) -> Result<Result<(), vertex::RemoveError>, TxnError>
    where
        V: ToVertexId,
    {
        vertex::txn_remove(&self.neb_txn, &self.schemas, vertex).await
    }
    pub async fn remove_vertex_by_key<K, S>(
        &self,
        schema: S,
        key: K,
    ) -> Result<Result<(), vertex::RemoveError>, TxnError>
    where
        K: ToValue,
        S: ToSchemaId,
    {
        let id = OwnedCell::encode_cell_key(schema.to_id(&self.schemas), &key.value());
        self.remove_vertex(id).await
    }

    pub async fn link<'b, V, S>(
        &self,
        from: V,
        schema: S,
        to: V,
        body: &Option<OwnedMap>,
    ) -> Result<Result<edge::Edge, LinkVerticesError>, TxnError>
    where
        V: ToVertexId,
        S: ToSchemaId,
    {
        let from_id = from.to_id();
        let to_id = to.to_id();
        let schema_id = schema.to_id(&self.schemas);
        let edge_attr = match self.schemas.schema_type(schema_id) {
            Some(GraphSchema::Edge(ea)) => ea,
            Some(_) => return Ok(Err(LinkVerticesError::SchemaNotEdge)),
            None => return Ok(Err(LinkVerticesError::EdgeSchemaNotFound)),
        };
        match edge_attr.edge_type {
            edge::EdgeType::Directed => Ok(edge::directed::DirectedEdge::link(
                from_id,
                to_id,
                body,
                &self.neb_txn,
                schema_id,
                &self.schemas,
            )
            .await?
            .map_err(LinkVerticesError::EdgeError)
            .map(edge::Edge::Directed)),

            edge::EdgeType::Undirected => Ok(edge::undirectd::UndirectedEdge::link(
                from_id,
                to_id,
                body,
                &self.neb_txn,
                schema_id,
                &self.schemas,
            )
            .await?
            .map_err(LinkVerticesError::EdgeError)
            .map(edge::Edge::Undirected)),
        }
    }

    pub async fn update_vertex<V, U>(&self, vertex: V, update: U) -> Result<(), TxnError>
    where
        V: ToVertexId,
        U: Fn(Vertex) -> Option<Vertex>,
    {
        vertex::txn_update(&self.neb_txn, vertex, &update).await
    }
    pub async fn update_vertex_by_key<K, U, S>(
        &self,
        schema: S,
        key: K,
        update: U,
    ) -> Result<(), TxnError>
    where
        K: ToValue,
        S: ToSchemaId,
        U: Fn(Vertex) -> Option<Vertex>,
    {
        let id = OwnedCell::encode_cell_key(schema.to_id(&self.schemas), &key.value());
        self.update_vertex(&id, update).await
    }

    pub async fn read_vertex<V>(&self, vertex: V) -> Result<Option<Vertex>, TxnError>
    where
        V: ToVertexId,
    {
        self.neb_txn
            .read(vertex.to_id())
            .await
            .map(|c| c.map(vertex::cell_to_vertex))
    }

    pub async fn get_vertex<K, S>(&self, schema: u32, key: K) -> Result<Option<Vertex>, TxnError>
    where
        K: ToValue,
        S: ToSchemaId,
    {
        let id = OwnedCell::encode_cell_key(schema.to_id(&self.schemas), &key.value());
        self.read_vertex(&id).await
    }

    pub async fn edges<'a, V, S>(
        &self,
        vertex: V,
        schema: S,
        ed: EdgeDirection,
        filter: &Option<Vec<SExpr<'a>>>,
    ) -> Result<Result<Vec<edge::Edge>, edge::EdgeError>, TxnError>
    where
        V: ToVertexId,
        S: ToSchemaId,
    {
        let vertex_field = ed.as_field();
        let schema_id = schema.to_id(&self.schemas);
        let vertex_id = vertex.to_id();
        match id_list::IdList::from_txn_and_container(
            &self.neb_txn,
            vertex_id,
            vertex_field,
            schema_id,
        )
        .iter()
        .await?
        {
            Err(e) => Ok(Err(edge::EdgeError::IdListError(e))),
            Ok(mut ids) => Ok(Ok({
                let mut edges = Vec::new();
                while let Some(id) = ids.next().await {
                    match edge::from_id(
                        vertex_id,
                        vertex_field,
                        schema_id,
                        &self.schemas,
                        &self.neb_txn,
                        id,
                    )
                    .await?
                    {
                        Ok(e) => match Tester::eval_with_edge(filter, &e).await {
                            Ok(true) => {
                                edges.push(e);
                            }
                            Ok(false) => {}
                            Err(err) => return Ok(Err(EdgeError::FilterEvalError(err))),
                        },
                        Err(er) => return Ok(Err(er)),
                    }
                }
                edges
            })),
        }
    }

    pub async fn neighbourhoods<'a, V, S>(
        &self,
        vertex: V,
        schema: S,
        ed: EdgeDirection,
        filter: &Option<Vec<SExpr<'a>>>,
    ) -> Result<Result<Vec<(Vertex, edge::Edge)>, NeighbourhoodError>, TxnError>
    where
        V: ToVertexId,
        S: ToSchemaId,
    {
        let vertex_field = ed.as_field();
        let schema_id = schema.to_id(&self.schemas);
        let vertex_id = &vertex.to_id();
        match id_list::IdList::from_txn_and_container(
            &self.neb_txn,
            *vertex_id,
            vertex_field,
            schema_id,
        )
        .iter()
        .await?
        {
            Err(e) => Ok(Err(NeighbourhoodError::EdgeError(EdgeError::IdListError(
                e,
            )))),
            Ok(mut ids) => {
                let mut result: Vec<(Vertex, edge::Edge)> = Vec::new();
                while let Some(id) = ids.next().await {
                    match edge::from_id(
                        *vertex_id,
                        vertex_field,
                        schema_id,
                        &self.schemas,
                        &self.neb_txn,
                        id,
                    )
                    .await?
                    {
                        Ok(edge) => {
                            let vertex = if let Some(opposite_id) =
                                edge.one_opposite_id_vertex_id(vertex_id).await
                            {
                                if let Some(v) = self.read_vertex(opposite_id).await? {
                                    v
                                } else {
                                    return Ok(Err(NeighbourhoodError::VertexNotFound(
                                        *opposite_id,
                                    )));
                                }
                            } else {
                                return Ok(Err(NeighbourhoodError::CannotFindOppositeId(
                                    *vertex_id,
                                )));
                            };
                            match Tester::eval_with_edge_and_vertex(filter, &vertex, &edge).await {
                                Ok(true) => {
                                    result.push((vertex, edge));
                                }
                                Ok(false) => {}
                                Err(err) => {
                                    return Ok(Err(NeighbourhoodError::FilterEvalError(err)))
                                }
                            }
                        }
                        Err(edge_error) => {
                            return Ok(Err(NeighbourhoodError::EdgeError(edge_error)))
                        }
                    }
                }
                return Ok(Ok(result));
            }
        }
    }

    pub async fn degree<V, S>(
        &self,
        vertex: V,
        schema: S,
        ed: EdgeDirection,
    ) -> Result<Result<usize, edge::EdgeError>, TxnError>
    where
        V: ToVertexId,
        S: ToSchemaId,
    {
        let (schema_id, edge_attr) = match edge_attr_from_schema(schema, &self.schemas) {
            Err(e) => return Ok(Err(e)),
            Ok(t) => t,
        };
        let vertex_field = ed.as_field();
        let vertex_id = vertex.to_id();
        match id_list::IdList::from_txn_and_container(
            &self.neb_txn,
            vertex_id,
            vertex_field,
            schema_id,
        )
        .count()
        .await?
        {
            Err(e) => Ok(Err(edge::EdgeError::IdListError(e))),
            Ok(count) => Ok(Ok(count)),
        }
    }
}

pub fn edge_attr_from_schema<S>(
    schema: S,
    schemas: &Arc<SchemaContainer>,
) -> Result<(u32, EdgeAttributes), EdgeError>
where
    S: ToSchemaId,
{
    let schema_id = schema.to_id(schemas);
    Ok((
        schema_id,
        match schemas.schema_type(schema_id) {
            Some(GraphSchema::Edge(ea)) => ea,
            Some(_) => return Err(EdgeError::WrongSchema),
            None => return Err(EdgeError::CannotFindSchema),
        },
    ))
}
