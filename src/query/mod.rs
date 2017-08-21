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
    fn to_sexpr(&self) -> Result<Vec<SExpr>, String>;
}

impl Expr for String {
    fn to_sexpr(&self) -> Result<Vec<SExpr>, String> {
        parse_to_expr(&self)
    }
}

impl <'a>Expr for &'a str {
    fn to_sexpr(&self) -> Result<Vec<SExpr>, String> {
        parse_to_expr(self)
    }
}

impl <'a>Expr for &'a Vec<SExpr> {
    fn to_sexpr(&self) -> Result<Vec<SExpr>, String> {
        return Ok(self.to_vec());
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

pub fn parse_optional_expr<E>(expr: &Option<E>)
    -> Result<Option<Vec<SExpr>>, String> where E: Expr {
    match expr {
        &Some(ref expr) => {
            let expr_owned = expr.clone();
            Ok(Some(expr_owned.to_sexpr()?))
        },
        &None => Ok(None)
    }
}

impl Tester {

    pub fn eval_with_edge_and_vertex(sexpr: &Option<Vec<SExpr>>, vertex: &Vertex, edge: &Edge)
        -> Result<bool, String> {
        let sexpr = sexpr.clone(); // TODO: Memory management
        let sexpr = if let Some(expr) = sexpr { expr } else { return Ok(true); };
        let interp = prep_interp();
        bind(VERTEX_SYMBOL, SExpr::Value(vertex.cell.data.clone()));
        bind(EDGE_SYMBOL, SExpr::Value(if let &Some(ref e) = edge.get_data() {
            e.data.clone()
        } else {Value::Null}));
        Ok(is_true(interp.eval(sexpr)?))
    }
    
    pub fn eval_with_vertex(sexpr: &Option<Vec<SExpr>>, vertex: &Vertex)
        -> Result<bool, String> {
        let sexpr = sexpr.clone(); // TODO: Memory management
        let sexpr = if let Some(expr) = sexpr { expr } else { return Ok(true); };
        let interp = prep_interp();
        bind(VERTEX_SYMBOL, SExpr::Value(vertex.cell.data.clone()));
        Ok(is_true(interp.eval(sexpr)?))
    }

    pub fn eval_with_edge(sexpr: &Option<Vec<SExpr>>, edge: &Edge)
        -> Result<bool, String> {
        let sexpr = sexpr.clone(); // TODO: Memory management
        let sexpr = if let Some(expr) = sexpr { expr } else { return Ok(true); };
        let interp = prep_interp();
        bind(EDGE_SYMBOL, SExpr::Value(if let &Some(ref e) = edge.get_data() {
            e.data.clone()
        } else {Value::Null}));
        Ok(is_true(interp.eval(sexpr)?))
    }
    
}