# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from kubernetes.client import (
    V1Container,
    V1ContainerPort,
    V1Deployment,
    V1DeploymentSpec,
    V1EnvVar,
    V1LabelSelector,
    V1ObjectMeta,
    V1PodSpec,
    V1PodTemplateSpec,
    V1Service,
    V1ServicePort,
    V1ServiceSpec,
)

from materialize.cloudtest import DEFAULT_K8S_NAMESPACE
from materialize.cloudtest.k8s.api.k8s_deployment import K8sDeployment
from materialize.cloudtest.k8s.api.k8s_resource import K8sResource
from materialize.cloudtest.k8s.api.k8s_service import K8sService


class PostgresService(K8sService):
    def __init__(
        self,
        namespace: str,
    ) -> None:
        super().__init__(namespace)
        service_port = V1ServicePort(name="sql", port=5432)

        self.service = V1Service(
            api_version="v1",
            kind="Service",
            metadata=V1ObjectMeta(
                name="postgres", namespace=namespace, labels={"app": "postgres"}
            ),
            spec=V1ServiceSpec(
                type="NodePort",
                ports=[service_port],
                selector={"app": "postgres"},
            ),
        )


class PostgresDeployment(K8sDeployment):
    def __init__(
        self,
        namespace: str,
    ) -> None:
        super().__init__(namespace)
        env = [
            V1EnvVar(name="POSTGRESDB", value="postgres"),
            V1EnvVar(name="POSTGRES_PASSWORD", value="postgres"),
        ]
        ports = [V1ContainerPort(container_port=5432, name="sql")]
        container = V1Container(
            name="postgres",
            image=self.image("postgres", tag=None, release_mode=True),
            args=["-c", "wal_level=logical"],
            env=env,
            ports=ports,
        )

        template = V1PodTemplateSpec(
            metadata=V1ObjectMeta(namespace=namespace, labels={"app": "postgres"}),
            spec=V1PodSpec(
                containers=[container], node_selector={"supporting-services": "true"}
            ),
        )

        selector = V1LabelSelector(match_labels={"app": "postgres"})

        spec = V1DeploymentSpec(replicas=1, template=template, selector=selector)

        self.deployment = V1Deployment(
            api_version="apps/v1",
            kind="Deployment",
            metadata=V1ObjectMeta(name="postgres", namespace=namespace),
            spec=spec,
        )


def postgres_resources(
    namespace: str = DEFAULT_K8S_NAMESPACE,
) -> list[K8sResource]:
    return [
        PostgresService(namespace),
        PostgresDeployment(namespace),
    ]
