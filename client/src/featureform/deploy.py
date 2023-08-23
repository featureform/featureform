
import os
import platform
from collections import namedtuple

import docker


DOCKER_CONFIG = namedtuple(
    "Docker_Config", ["name", "image", "port", "detach_mode", "env"]
)


class Deployment:
    def __init__(self, quickstart: bool):
        self._quickstart = quickstart
        self._status = None
        self._config = []

    def start(self) -> bool:
        pass

    def stop(self) -> bool:
        pass

    def health_check(self):
        pass

    @property
    def status(self) -> str:
        return self._status

    @property
    def config(self) -> list:
        return self._config


class DockerDeployment(Deployment):
    # TODO: Add support for custom ports, better formatted text output
    def __init__(self, quickstart: bool):
        super().__init__(quickstart)
        self._client = docker.from_env()

        environment_variables = {}

        is_mac_m1_chip = platform.machine() == "arm64" and platform.system() == "Darwin"
        if is_mac_m1_chip:
            environment_variables["ETCD_ARCH"] = "ETCD_UNSUPPORTED_ARCH=arm64"

        # TODO: Add support for custom ports
        featureform_deployment = DOCKER_CONFIG(
            name="featureform",
            image=os.getenv("FEATUREFORM_DOCKER_IMAGE","featureformcom/featureform:latest"),
            port={"7878/tcp": 7878, "80/tcp": 80},
            detach_mode=True,
            env=environment_variables,
        )

        # TODO: Add support for custom ports
        quickstart_deployment = [
            DOCKER_CONFIG(
                name="quickstart-postgres",
                image="featureformcom/postgres",
                port={"5432/tcp": 5432},
                detach_mode=True,
                env={},
            ),
            DOCKER_CONFIG(
                name="quickstart-redis",
                image="redis:latest",
                port={"6379/tcp": 6379},
                detach_mode=True,
                env={},
            ),
        ]

        if self._quickstart:
            self._config = [featureform_deployment] + quickstart_deployment
        else:
            self._config = [featureform_deployment]

    def start(self) -> bool:
        print("Starting containers...")
        for config in self._config:
            try:
                print(f"Checking if {config.name} container exists...")
                container = self._client.containers.get(config.name)
                if container.status == "running":
                    continue
                elif container.status == "exited":
                    container.start()
            except docker.errors.APIError as e:
                if e.status_code == 409:
                    print(f"Container {config.name} already exists. Skipping...")
                    continue
                elif e.status_code == 404:
                    print(
                        f"Container {config.name} not found. Creating new container..."
                    )
                    container = self._client.containers.run(
                        name=config.name,
                        image=config.image,
                        ports=config.port,
                        detach=config.detach_mode,
                        environment=config.env,
                    )
                    print(f"'{container.name}' container started")
                else:
                    print("Error starting container: ", e)
                    return False
        return True

    def stop(self) -> bool:
        print("Stopping containers...")
        for config in self._config:
            try:
                container = self._client.containers.get(config.name)
                if container.status == "running":
                    container.stop()
            except docker.errors.NotFound:
                print(f"Container {config.name} not found. Skipping...")
                continue
            except docker.errors.APIError as e:
                print("Error stopping container: ", e)
                return False
        return True
