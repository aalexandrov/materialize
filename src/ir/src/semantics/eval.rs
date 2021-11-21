use repr::Row;

use crate::syntax::*;

pub struct Eval;

impl TypScalar for Eval {
    type Scalar = Row;
}
