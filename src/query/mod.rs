use neb::dovahkiin::expr::interpreter::Interpreter;
use neb::dovahkiin::expr::symbols::bindings::bind;
use neb::dovahkiin::expr::SExpr;
use neb::dovahkiin::expr::symbols::utils::is_true;
use neb::dovahkiin::integrated::lisp::parse_to_expr;
use neb::dovahkiin::types::Value;
use graph::edge::Edge;
use graph::vertex::Vertex;

pub static VERTEX_SYMBOL: u64 = hash_ident!(vertex) as u64;
pub static EDGE_SYMBOL: u64 = hash_ident!(edge) as u64;

pub trait Expr {
    fn to_sexpr(self) -> Result<Vec<SExpr>, String>;
}

impl Expr for String {
    fn to_sexpr(self) -> Result<Vec<SExpr>, String> {
        parse_to_expr(&self)
    }
}

impl <'a>Expr for &'a str {
    fn to_sexpr(self) -> Result<Vec<SExpr>, String> {
        parse_to_expr(self)
    }
}

impl Expr for Vec<SExpr> {
    fn to_sexpr(self) -> Result<Vec<SExpr>, String> {
        return Ok(self);
    }
}

impl Expr for SExpr {
    fn to_sexpr(self) -> Result<Vec<SExpr>, String> {
        Ok(vec![self])
    }
}

pub struct Tester {
    core: Interpreter
}

fn prep_interp() -> Interpreter {
    let inter = Interpreter::new();
    inter.set_env();
    return inter;
}

impl Tester {

    pub fn eval_with_edge_and_vertex<E>(expr: Option<E>, vertex: &Vertex, edge: &Edge) -> Result<bool, String>
        where E: Expr {
        let sexpr = if let Some(expr) = expr { expr.to_sexpr()? } else { return Ok(true); };
        let interp = prep_interp();
        bind(VERTEX_SYMBOL, SExpr::Value(vertex.cell.data.clone()));
        bind(EDGE_SYMBOL, SExpr::Value(if let &Some(ref e) = edge.get_data() {
            e.data.clone()
        } else {Value::Null}));
        Ok(is_true(interp.eval(sexpr)?))
    }
    
    pub fn eval_with_vertex<E>(expr: Option<E>, vertex: &Vertex) -> Result<bool, String>
        where E: Expr {
        let sexpr = if let Some(expr) = expr { expr.to_sexpr()? } else { return Ok(true); };
        let interp = prep_interp();
        bind(VERTEX_SYMBOL, SExpr::Value(vertex.cell.data.clone()));
        Ok(is_true(interp.eval(sexpr)?))
    }

    pub fn eval_with_edge<E>(expr: Option<E>, edge: &Edge) -> Result<bool, String>
        where E: Expr {
        let sexpr = if let Some(expr) = expr { expr.to_sexpr()? } else { return Ok(true); };
        let interp = prep_interp();
        bind(EDGE_SYMBOL, SExpr::Value(if let &Some(ref e) = edge.get_data() {
            e.data.clone()
        } else {Value::Null}));
        Ok(is_true(interp.eval(sexpr)?))
    }
    
}