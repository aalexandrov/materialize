// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Derived attributes framework and definitions.

use std::collections::HashMap;

use mz_expr::{LocalId, MirRelationExpr};

pub mod non_negative;
pub mod subtree_size;

pub trait Attribute {
    /// The domain of the attribute values.
    type Value: Clone + Eq + PartialEq;
    /// The domain of the dependencies.
    type Dependencies;

    /// Derive an attribute.
    fn derive(&mut self, expr: &MirRelationExpr, deps: &Self::Dependencies);

    /// Schedule environment maintenance tasks.
    ///
    /// Deletate to [`Env::schedule_tasks`] if this attribute has an [`Env`] field.
    fn schedule_env_tasks(&mut self, _expr: &MirRelationExpr) {}

    /// Handle scheduled environment maintenance tasks.
    ///
    /// Deletate to [`Env::handle_tasks`] if this attribute has an [`Env`] field.
    fn handle_env_tasks(&mut self) {}
}

/// A map that keeps the computed `A` values for all `LocalId`
/// bindings in the current environment.
#[derive(Default)]
#[allow(missing_debug_implementations)]
pub struct Env<A: Attribute> {
    /// The [`HashMap`] backing this environment.
    env: HashMap<LocalId, A::Value>,
    // A stack of tasks to maintain the `env` map.
    env_tasks: Vec<EnvTask<A::Value>>,
}

impl<A: Attribute> Env<A> {
    pub(crate) fn get(&self, id: &LocalId) -> Option<&A::Value> {
        self.env.get(id)
    }
}

impl<A: Attribute> Env<A> {
    /// Schedules evinronment maintenance tasks.
    ///
    /// Should be called from a `Visitor<MirRelationExpr>::pre_visit` implementaion.
    pub fn schedule_tasks(&mut self, expr: &MirRelationExpr) {
        match expr {
            MirRelationExpr::Let { id, .. } => {
                // Add an Extend task to be handled in the post_visit the node children.
                self.env_tasks.push(EnvTask::Extend(id.clone()));
            }
            _ => {
                // Don not do anything with the environment in this node's children.
                self.env_tasks.push(EnvTask::NoOp);
            }
        }
    }

    /// Handles scheduled evinronment maintenance tasks.
    ///
    /// Should be called from a `Visitor<MirRelationExpr>::post_visit` implementaion.
    pub fn handle_tasks(&mut self, results: &Vec<A::Value>) {
        // Pop the env task for this element's children.
        let parent = self.env_tasks.pop();
        // This should be always be NoOp, as this is either the original value or the
        // terminal state of the state machine that Let children implement (see the
        // code at the end of this method).
        debug_assert!(parent.is_some() && parent.unwrap() == EnvTask::NoOp);

        // Handle EnvTask initiated by the parent.
        if let Some(env_task) = self.env_tasks.pop() {
            // Compute a task to represent the next state of the state machine associated with
            // this task.
            let env_task = match env_task {
                // An Extend indicates that the parent of the current node is a Let binding
                // and we are about to leave a Let binding value.
                EnvTask::Extend(id) => {
                    // Before descending to the next sibling (the Let binding body), do as follows:
                    // 1. Update the env map with the attribute derived for the Let binding value.
                    let result = results.last().expect("unexpected empty results vector");
                    let oldval = self.env.insert(id, result.clone());
                    // 2. Create a task to be handled after visiting the Let binding body.
                    match oldval {
                        Some(val) => EnvTask::Reset(id, val), // reset old value if present
                        None => EnvTask::Remove(id),          // remove key otherwise
                    }
                }
                // An Reset task indicates that we are about to leave the Let binding body
                // and the id of the Let parent shadowed another `id` in the environment.
                EnvTask::Reset(id, val) => {
                    // Before moving to the post_visit of the enclosing Let parent, do as follows:
                    // 1. Reset the entry in the env map with the shadowed value.
                    self.env.insert(id, val);
                    // 2. Create a NoOp task indicating that there is nothing more to be done.
                    EnvTask::NoOp
                }
                // An Remove task indicates that we are about to leave the Let binding body
                // and the id of the Let parent did not shadow another `id` in the environment.
                EnvTask::Remove(id) => {
                    // Before moving to the post_visit of the enclosing Let parent, do as follows:
                    // 1. Remove the value assciated with the given `id` from the environment.
                    self.env.remove(&id);
                    // 2. Create a NoOp task indicating that there is nothing more to be done.
                    EnvTask::NoOp
                }
                // A NoOp task indicates that we don't need to do anyting.
                EnvTask::NoOp => EnvTask::NoOp,
            };
            // Advance the state machine.
            self.env_tasks.push(env_task);
        };
    }
}

/// Models an environment maintenence task that needs to be executed
/// after visiting a [`MirRelationExpr`].
///
/// The [`Env::schedule_tasks`] hook installs such a task for each node,
/// and the [`Env::handle_tasks`] hook removes it.
///
/// In addition, the [`Env::handle_tasks`] looks at the task installed
/// by its parent and modifies it if needed, advancing it through the
/// following state machinge from left to right:
/// ```text
/// Set --- Reset ---- NoOp
///     \            /
///      -- Remove --
/// ```
#[derive(Eq, PartialEq, Debug)]
enum EnvTask<T> {
    /// Add the latest attribute to the environment under the given key.
    Extend(LocalId),
    /// Reset value of an environment entry.
    Reset(LocalId, T),
    /// Remove an entry from the environment.
    Remove(LocalId),
    /// Do not do anything.
    NoOp,
}

mod newapi {

    use differential_dataflow::algorithms;
    use mz_expr::MirRelationExpr;
    use std::{any::Any, collections::HashSet};

    use super::{non_negative::NonNegativeSpec, Env};

    // TODO rename to Attribute
    /// A common interface to be implemented by all derived attributes.
    pub trait AttributeSpec: Eq + Clone + std::hash::Hash + Into<AttributeAlgorithm<Self>> {
        /// The domain of the attribute values.
        type Value: Clone + Eq + PartialEq;

        /// A vector of attributes that need to be derived before
        /// this attribute.
        fn requires(&self) -> Vec<Attribute> {
            vec![] // FIXME
        }
    }

    pub struct AttributeAlgorithm<S: AttributeSpec + Sized> {
        pub results: Vec<S::Value>,
        pub env: (), // TODO
    }

    #[derive(Eq, PartialEq, Clone, Hash)]
    pub enum Attribute {
        NonNegative(NonNegativeSpec), // TODO
        SubtreeSize(NonNegativeSpec), // TODO
    }

    impl Attribute {
        /// A vector of attributes that need to  be derived before
        /// this attribute.
        fn requires(&self) -> Vec<Attribute> {
            todo!()
            // match self {
            //     Attribute::NonNegative(attr) => attr.requires(),
            //     Attribute::SubtreeSize(attr) => attr.requires(),
            // }
        }

        fn algorithm(self) -> Box<dyn Any> {
            let algorithm = match self {
                Attribute::NonNegative(spec) => {
                    Into::<AttributeAlgorithm<NonNegativeSpec>>::into(spec)
                }
                Attribute::SubtreeSize(spec) => {
                    Into::<AttributeAlgorithm<NonNegativeSpec>>::into(spec)
                }
            };
            Box::new(algorithm)
        }
    }

    /// A struct that represents an [`Attribute`] set that needs
    /// to be present for some follow-up logic (most likely
    /// transformation, but can also be pretty-printing or something
    /// else).
    pub struct RequiredAttributes {
        attributes: Vec<Attribute>,
        algorithms: Vec<Box<dyn Any>>,
    }

    impl From<HashSet<Attribute>> for RequiredAttributes {
        /// Completes the set attributes with transitive dependencies
        /// and wraps the result in a representation that is suitable
        /// for attribute derivation in a minimum number of passes.
        fn from(mut attributes: HashSet<Attribute>) -> Self {
            // add missing dependencies required to derive this set of attributes
            transitive_closure(&mut attributes);
            // order transitive closure topologically based on dependency order
            let attributes = dependency_order(attributes);
            // derive algorithms for the corresponding attributes
            let algorithms = attributes
                .iter()
                .cloned()
                .map(Attribute::algorithm)
                .collect::<Vec<_>>();

            // wrap resulting vector a new RequiredAttributes instance
            RequiredAttributes {
                attributes,
                algorithms,
            }
        }
    }

    impl RequiredAttributes {
        /// Derive attributes for the entire model.
        ///
        /// The currently implementation assumes that all attributes
        /// can be derived in a single bottom up pass.
        pub(crate) fn derive(&self, expr: &MirRelationExpr) {
            // while let Some((head, tail)) = attributes.split_first_mut() {
            //     attributes = tail;
            // }

            todo!()
        }
    }

    /// Consumes a set of attributes and produces a topologically sorted
    /// version of the elements in that set based on the dependency
    /// information provided by the [`Attribute::requires`] results.
    ///
    /// We use Kahn's algorithm[^1] to sort the input.
    ///
    /// [^1]: <https://en.wikipedia.org/wiki/Topological_sorting#Kahn's_algorithm>
    fn dependency_order(attributes: HashSet<Attribute>) -> Vec<Attribute> {
        let mut rest = attributes.into_iter().collect::<Vec<_>>();
        let mut seen = HashSet::new() as HashSet<Attribute>;
        let mut sort = vec![] as Vec<Attribute>;

        while !rest.is_empty() {
            let (tail, head) = rest.into_iter().partition::<Vec<_>, _>(|attr| {
                attr.requires()
                    .into_iter()
                    .filter(|req| !seen.contains(req))
                    .next()
                    .is_some()
            });
            rest = tail;
            seen.extend(head.clone());
            sort.extend(head);
        }

        sort
    }

    /// Compute the transitive closure of the given set of attributes.
    fn transitive_closure(attributes: &mut HashSet<Attribute>) {
        let mut diff = requirements(&attributes);

        // iterate until no new attributes can be discovered
        while !diff.is_empty() {
            attributes.extend(diff);
            diff = requirements(&attributes);
        }
    }

    /// Compute the attributes required to derive the given set of `attributes` that are not
    /// already in that set.
    fn requirements(attributes: &HashSet<Attribute>) -> HashSet<Attribute> {
        attributes
            .iter()
            .flat_map(|a| a.requires())
            .filter(|a| !attributes.contains(a))
            .collect::<HashSet<_>>()
    }
}
