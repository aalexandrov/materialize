pub mod aggregate;
pub mod relation;
pub mod scalar;

use ir_tools::sorts;

sorts![Scalar, Aggregate, Relation];
