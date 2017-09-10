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

// (select-cell "<schema>" <cell-id>)
// (select-cell "<schema>" (hashmap "<key>" <value>)) // until index is done
#[derive(Debug)]
pub struct Select {}
impl Symbol for Select {
    fn eval(&self, exprs: Vec<SExpr>) -> Result<SExpr, String> {
        unimplemented!();
    }
    fn is_macro(&self) -> bool { true }
}

// (update-cell "<schema>" <cell-id> (hashmap ...))
// (update-cell "<schema>" (hashmap ...) (hashmap ...)) // until index is done
#[derive(Debug)]
pub struct Update {}
impl Symbol for Update {
    fn eval(&self, exprs: Vec<SExpr>) -> Result<SExpr, String> {
        unimplemented!();
    }
    fn is_macro(&self) -> bool { true }
}

// (delete-cell "<schema>" <cell-id>)
// (delete-cell "<schema>" (hashmap ...)) // until index is done
#[derive(Debug)]
pub struct Delete {}
impl Symbol for Delete {
    fn eval(&self, exprs: Vec<SExpr>) -> Result<SExpr, String> {
        unimplemented!();
    }
    fn is_macro(&self) -> bool { true }
}