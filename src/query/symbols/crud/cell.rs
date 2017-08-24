use neb::dovahkiin::expr::symbols::Symbol;
use neb::dovahkiin::expr::SExpr;

// (insert-cell "<schema>" (hashmap "<key-1>" <value-1> "<key-2>" (hashmap "<key-3>" <value-2>)))
#[derive(Debug)]
pub struct Insert {}
impl Symbol for Insert {
    fn eval(&self, exprs: Vec<SExpr>) -> Result<SExpr, String> {
        unimplemented!();
    }
    fn is_macro(&self) -> bool { true }
}

#[derive(Debug)]
pub struct Select {}
impl Symbol for Select {
    fn eval(&self, exprs: Vec<SExpr>) -> Result<SExpr, String> {
        unimplemented!();
    }
    fn is_macro(&self) -> bool { true }
}

#[derive(Debug)]
pub struct Update {}
impl Symbol for Update {
    fn eval(&self, exprs: Vec<SExpr>) -> Result<SExpr, String> {
        unimplemented!();
    }
    fn is_macro(&self) -> bool { true }
}

#[derive(Debug)]
pub struct Delete {}
impl Symbol for Delete {
    fn eval(&self, exprs: Vec<SExpr>) -> Result<SExpr, String> {
        unimplemented!();
    }
    fn is_macro(&self) -> bool { true }
}