pub mod from_hir;
pub mod from_mir;
pub mod to_hir;
pub mod to_mir;

fn from_vec<T: Clone, U: From<T>>(vec: &Vec<T>) -> Vec<U> {
    vec.iter().map(|e| e.clone().into()).collect()
}

fn from_vec_vec<T: Clone, U: From<T>>(vec: &Vec<Vec<T>>) -> Vec<Vec<U>> {
    vec.iter().map(|v| from_vec(v)).collect()
}
