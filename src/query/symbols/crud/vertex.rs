use dovahkiin::expr::interpreter::Envorinment;
use neb::dovahkiin::expr::symbols::Symbol;
use neb::dovahkiin::expr::SExpr;

#[derive(Debug)]
pub struct Insert {}
impl Symbol for Insert {
    fn eval<'a>(
        &self,
        exprs: Vec<SExpr<'a>>,
        env: &mut Envorinment<'a>,
    ) -> Result<SExpr<'a>, String> {
        unimplemented!();
    }
    fn is_macro(&self) -> bool {
        true
    }
}

#[derive(Debug)]
pub struct Select {}
impl Symbol for Select {
    fn eval<'a>(
        &self,
        exprs: Vec<SExpr<'a>>,
        env: &mut Envorinment<'a>,
    ) -> Result<SExpr<'a>, String> {
        unimplemented!();
    }
    fn is_macro(&self) -> bool {
        true
    }
}

#[derive(Debug)]
pub struct Update {}
impl Symbol for Update {
    fn eval<'a>(
        &self,
        exprs: Vec<SExpr<'a>>,
        env: &mut Envorinment<'a>,
    ) -> Result<SExpr<'a>, String> {
        unimplemented!();
    }
    fn is_macro(&self) -> bool {
        true
    }
}

#[derive(Debug)]
pub struct Delete {}
impl Symbol for Delete {
    fn eval<'a>(
        &self,
        exprs: Vec<SExpr<'a>>,
        env: &mut Envorinment<'a>,
    ) -> Result<SExpr<'a>, String> {
        unimplemented!();
    }
    fn is_macro(&self) -> bool {
        true
    }
}
