In general we now have

```rust
impl<T: VisitChildren<T>> Visit for T { ... }
```

which provides recursive implementations of pre- and post-visit methods (mutable or not, fallibe or not). Those are made recursion-safe with a `checked_recur` guard and consequently all variants return a `Result<_, _>` in order to provision for the possible `RecursionLimitError`.

I propose to rename `Visit` to `VisitRec` to add a non-recursive version

```rust
/// Iterative visitor methods.
///
/// Callers of the methods proided by this trait should not rely on a
/// specific visit order.
///
/// The trait should be preferred over [`VisitIter`] because it cannot
/// lead to stack overflows when processing deep plans.
trait VisitIter {
    /// Iterative immutable infallible visitor for `self`.
    fn visit_post<F>(&self, f: &mut F)
    where
        F: FnMut(&Self);

    /// Iterative mutable infallible visitor for `self`.
    fn visit_mut<F>(&mut self, f: &mut F)
    where
        F: FnMut(&mut Self);

    /// Iterative immutable fallible visitor for `self`.
    fn try_visit<F, E>(&self, f: &mut F) -> Result<(), E>
    where
        F: FnMut(&Self) -> Result<(), E>;

    /// Iterative mutable fallible visitor for `self`.
    fn try_visit_mut<F, E>(&mut self, f: &mut F) -> Result<(), E>
    where
        F: FnMut(&mut Self) -> Result<(), E>;
}
```

which has a similar blanket implementation

```rust
impl<T: IterChildren<T>> VisitIter for T { ... }
```

The iterative definitions will be based on the `children()` iterator provided by `IterChildren`.

However, I'm not 100% sure how this will work for `HirRelationExpr` where we need to also include `HirRelationExpr` wrapped in `Select` or `Exists` expressions in the list of children. This seems very doable when iterating over immutable references, but might not be so easy to define for mutable references.
