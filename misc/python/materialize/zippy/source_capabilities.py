# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.zippy.framework import Capability
from materialize.zippy.kafka_capabilities import TopicExists
from materialize.zippy.watermarks import Watermarks


class SourceExists(Capability):
    """A Kafka source exists in Materialize."""

    @classmethod
    def format_str(cls) -> str:
        return "source_{}"

    def __init__(self, name: str, topic: TopicExists, cluster_name: str) -> None:
        self.name = name
        self.topic = topic
        self.cluster_name = cluster_name

    def get_watermarks(self) -> Watermarks:
        return self.topic.watermarks
