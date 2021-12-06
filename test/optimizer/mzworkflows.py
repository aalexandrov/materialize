# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import mzbench_opt.cli
import mzbench_opt.sql

from materialize.mzcompose import Materialized, Workflow

mz = Materialized(name="mz_default", hostname="materialized")

services = [
    mz,
]


def workflow_test_optimizer(w: Workflow):
    w.start_and_wait_for_tcp(services=[mz])
    w.wait_for_mz(service=mz.name)

    scenario = mzbench_opt.sql.Scenario.TPCH
    samples = 201
    db_host = mz.config["hostname"]

    mzbench_opt.cli.init(scenario, db_host=db_host)
    mzbench_opt.cli.run(scenario, db_host=db_host, samples=samples)

    w.kill_services(services=[mz.name])
