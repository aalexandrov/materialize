# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


import logging

import pytest

from materialize.checks.actions import Action, Initialize, Manipulate, Validate
from materialize.checks.all_checks import *  # noqa: F401 F403
from materialize.checks.all_checks.ssh import SshKafka, SshPg
from materialize.checks.checks import Check
from materialize.checks.cloudtest_actions import ReplaceEnvironmentdStatefulSet
from materialize.checks.executors import CloudtestExecutor
from materialize.checks.scenarios import Scenario
from materialize.cloudtest.app.materialize_application import MaterializeApplication
from materialize.cloudtest.util.wait import wait
from materialize.util import MzVersion, all_subclasses
from materialize.version_list import VersionsFromDocs

LOGGER = logging.getLogger(__name__)

LAST_RELEASED_VERSION = VersionsFromDocs().minor_versions()[-1]


class CloudtestUpgrade(Scenario):
    """A Platform Checks scenario that performs an upgrade in cloudtest/K8s"""

    def base_version(self) -> MzVersion:
        return LAST_RELEASED_VERSION

    def actions(self) -> list[Action]:
        return [
            Initialize(self),
            Manipulate(self, phase=1),
            ReplaceEnvironmentdStatefulSet(new_tag=None),
            Manipulate(self, phase=2),
            Validate(self),
        ]


@pytest.mark.long
def test_upgrade(aws_region: str | None, log_filter: str | None, dev: bool) -> None:
    """Test upgrade from the last released verison to the current source by running all the Platform Checks"""
    LOGGER.info(
        f"Testing upgrade from base version {LAST_RELEASED_VERSION} to current version"
    )

    mz = MaterializeApplication(
        tag=str(LAST_RELEASED_VERSION),
        aws_region=aws_region,
        log_filter=log_filter,
        release_mode=(not dev),
    )
    wait(
        condition="condition=Ready",
        resource="pod",
        label="cluster.environmentd.materialize.cloud/cluster-id=u1",
    )

    executor = CloudtestExecutor(application=mz, version=LAST_RELEASED_VERSION)
    # No SSH bastion host in cloudtest (yet)
    checks = list(all_subclasses(Check) - {SshPg, SshKafka})
    scenario = CloudtestUpgrade(checks=checks, executor=executor)
    scenario.run()
