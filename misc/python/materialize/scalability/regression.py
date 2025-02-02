# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from __future__ import annotations

from materialize.scalability.endpoint import Endpoint


class Regression:
    def __init__(
        self,
        workload_name: str,
        concurrency: int,
        count: int,
        tps: float,
        tps_baseline: float,
        tps_diff: float,
        tps_diff_percent: float,
        endpoint: Endpoint,
    ):
        self.workload_name = workload_name
        self.concurrency = concurrency
        self.count = count
        self.tps = tps
        self.tps_baseline = tps_baseline
        assert tps_diff < 0, "Not a regression!"
        self.tps_diff = tps_diff
        self.tps_diff_percent = tps_diff_percent
        self.endpoint = endpoint

    def __str__(self) -> str:
        return (
            f"Regression in workload '{self.workload_name}' at concurrency {self.concurrency} with {self.endpoint}:"
            f" {round(self.tps, 2)} tps vs. {round(self.tps_baseline, 2)} tps"
            f" ({round(self.tps_diff, 2)} tps; {round(100 * self.tps_diff_percent, 2)}%)"
        )
