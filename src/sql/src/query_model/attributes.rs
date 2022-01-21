use std::ops::BitAnd;
use typenum::*;

type IfThenElse<B, T, E> = <B as Cond<T, E>>::Output;

trait Cond<T, E>
where
    Self: Bit,
{
    type Output;
}

impl<T, E> Cond<T, E> for B0 {
    type Output = E;
}

impl<T, E> Cond<T, E> for B1 {
    type Output = T;
}

trait AttrSpec {
    type A1;
    type A2;
    type A3;
}

impl<U> AttrSpec for U
where
    U: Unsigned,
    U: BitAnd<U1>,
    U: BitAnd<U2>,
    U: BitAnd<U4>,
    And<U, U1>: IsEqual<U1>,
    And<U, U2>: IsEqual<U2>,
    And<U, U4>: IsEqual<U4>,
    Eq<And<U, U1>, U1>: Cond<Attr1, ()>,
    Eq<And<U, U2>, U2>: Cond<Attr2, ()>,
    Eq<And<U, U4>, U4>: Cond<Attr3, ()>,
{
    type A1 = IfThenElse<Eq<And<U, U1>, U1>, Attr1, ()>;
    type A2 = IfThenElse<Eq<And<U, U2>, U2>, Attr2, ()>;
    type A3 = IfThenElse<Eq<And<U, U4>, U4>, Attr3, ()>;
}

#[allow(dead_code)]
type Attr1Id = U1;
#[allow(dead_code)]
type Attr2Id = U2;
#[allow(dead_code)]
type Attr3Id = U4;

struct Attr1(usize);
struct Attr2(usize);
struct Attr3(usize);

#[allow(dead_code)]
struct Attrs<U: AttrSpec> {
    a1: U::A1,
    a2: U::A2,
    a3: U::A3,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn example2() {
        let _attrs1 = Attrs::<U0> {
            a1: (),
            a2: (),
            a3: (),
        };

        let _attrs2 = Attrs::<op!(Attr1Id | Attr2Id)> {
            a1: Attr1(1),
            a2: Attr2(1),
            a3: (),
        };

        let _attrs3 = Attrs::<op!(Attr1Id | Attr3Id)> {
            a1: Attr1(1),
            a2: (),
            a3: Attr3(1),
        };
    }
}
