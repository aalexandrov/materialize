// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module houses a pretty printer for the parts of a
//! [`DataflowDescription`] that are relevant to dataflow rendering.
//!
//! Format details:
//!
//!   * Sources that have [`MapFilterProject`]s come first.
//!     The format is "Source <name> (<id>):" followed by the [`MapFilterProject`].
//!   * Intermediate views in the dataflow come next.
//!     The format is "View <name> (<id>):" followed by the output of
//!     [`mz_expr::explain::ViewExplanation`].
//!   * Last is the view or query being explained. The format is "Query:"
//!     followed by the output of [`mz_expr::explain::ViewExplanation`].
//!   * If there are no sources with some [`MapFilterProject`] and no intermediate
//!     views, then the format is identical to the format of
//!     [`mz_expr::explain::ViewExplanation`].
//!
//! It's important to avoid trailing whitespace everywhere, as plans may be
//! printed in contexts where trailing whitespace is unacceptable, like
//! sqllogictest files.

use std::fmt;

use chrono::NaiveDateTime;
use mz_storage::types::sources::Timeline;

/// Information used when determining the timestamp for a query.
pub struct TimestampExplanation<T> {
    /// The chosen timestamp from `determine_timestamp`.
    pub timestamp: T,
    /// The timeline that the timestamp corresponds to.
    pub timeline: Option<Timeline>,
    /// The read frontier of all involved sources.
    pub since: Vec<T>,
    /// The write frontier of all involved sources.
    pub upper: Vec<T>,
    /// Whether the query can responded immediately or if it has to block.
    pub respond_immediately: bool,
    /// The current value of the global timestamp.
    pub global_timestamp: T,
    /// Details about each source.
    pub sources: Vec<TimestampSource<T>>,
}

pub struct TimestampSource<T> {
    pub name: String,
    pub read_frontier: Vec<T>,
    pub write_frontier: Vec<T>,
}

pub trait DisplayableInTimeline {
    fn fmt(&self, timeline: Option<&Timeline>, f: &mut fmt::Formatter) -> fmt::Result;
    fn display<'a>(&'a self, timeline: Option<&'a Timeline>) -> DisplayInTimeline<'a, Self> {
        DisplayInTimeline { t: self, timeline }
    }
}

impl DisplayableInTimeline for mz_repr::Timestamp {
    fn fmt(&self, timeline: Option<&Timeline>, f: &mut fmt::Formatter) -> fmt::Result {
        match timeline {
            Some(Timeline::EpochMilliseconds) => {
                let ts_ms: u64 = self.into();
                let ts = ts_ms / 1000;
                let nanos = ((ts_ms % 1000) as u32) * 1000000;
                let ndt = NaiveDateTime::from_timestamp(ts as i64, nanos);
                write!(f, "{:13} ({})", self, ndt.format("%Y-%m-%d %H:%M:%S%.3f"))
            }
            None | Some(_) => {
                write!(f, "{:13}", self)
            }
        }
    }
}

pub struct DisplayInTimeline<'a, T: ?Sized> {
    t: &'a T,
    timeline: Option<&'a Timeline>,
}
impl<'a, T> fmt::Display for DisplayInTimeline<'a, T>
where
    T: DisplayableInTimeline,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.t.fmt(self.timeline, f)
    }
}

impl<'a, T> fmt::Debug for DisplayInTimeline<'a, T>
where
    T: DisplayableInTimeline,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self, f)
    }
}

impl<T: fmt::Display + fmt::Debug + DisplayableInTimeline> fmt::Display
    for TimestampExplanation<T>
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let timeline = self.timeline.as_ref();
        writeln!(
            f,
            "          query timestamp: {}",
            self.timestamp.display(timeline)
        )?;
        writeln!(
            f,
            "                    since:{:?}",
            self.since
                .iter()
                .map(|t| t.display(timeline))
                .collect::<Vec<_>>()
        )?;
        writeln!(
            f,
            "                    upper:{:?}",
            self.upper
                .iter()
                .map(|t| t.display(timeline))
                .collect::<Vec<_>>()
        )?;
        writeln!(
            f,
            "         global timestamp: {}",
            self.global_timestamp.display(timeline)
        )?;
        writeln!(f, "  can respond immediately: {}", self.respond_immediately)?;
        for source in &self.sources {
            writeln!(f, "")?;
            writeln!(f, "source {}:", source.name)?;
            writeln!(
                f,
                "            read frontier:{:?}",
                source
                    .read_frontier
                    .iter()
                    .map(|t| t.display(timeline))
                    .collect::<Vec<_>>()
            )?;
            writeln!(
                f,
                "           write frontier:{:?}",
                source
                    .write_frontier
                    .iter()
                    .map(|t| t.display(timeline))
                    .collect::<Vec<_>>()
            )?;
        }
        Ok(())
    }
}
