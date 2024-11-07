#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import os
import platform
import warnings
from collections import namedtuple

import docker
import requests


warnings.simplefilter("ignore")

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
    def __init__(self, quickstart: bool, clickhouse: bool = False):
        super().__init__(quickstart)

        self._quickstart_directory = "quickstart"
        self._quickstart_files = [
            "https://featureform-demo-files.s3.amazonaws.com/definitions.py",
            "https://featureform-demo-files.s3.amazonaws.com/serving.py",
            "https://featureform-demo-files.s3.amazonaws.com/training.py",
        ]

        try:
            self._client = docker.from_env()
        except docker.errors.DockerException:
            raise Exception("Error connecting to Docker daemon. Is Docker running?")

        environment_variables = {}
        is_mac_m1_chip = platform.machine() == "arm64" and platform.system() == "Darwin"
        if is_mac_m1_chip:
            environment_variables["ETCD_ARCH"] = "ETCD_UNSUPPORTED_ARCH=arm64"

        # TODO: Add support for custom ports
        featureform_deployment = DOCKER_CONFIG(
            name="featureform",
            image=os.getenv(
                "FEATUREFORM_DOCKER_IMAGE", "featureformcom/featureform:latest"
            ),
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
        if clickhouse:
            quickstart_deployment.append(
                DOCKER_CONFIG(
                    name="quickstart-clickhouse",
                    image="clickhouse/clickhouse-server",
                    port={"9000/tcp": 9000, "8123/tcp": 8123},
                    detach_mode=True,
                    env={},
                )
            )

        if self._quickstart:
            self._config = [featureform_deployment] + quickstart_deployment
        else:
            self._config = [featureform_deployment]

    def start(self) -> bool:
        print(f"Starting Docker deployment on {platform.system()} {platform.release()}")
        for config in self._config:
            try:
                print(f"Checking if {config.name} container exists...")
                container = self._client.containers.get(config.name)
                if container.status == "running":
                    print(f"\tContainer {config.name} is already running. Skipping...")
                    continue
                elif container.status == "exited":
                    print(f"\tContainer {config.name} is stopped. Starting...")
                    container.start()
            except docker.errors.APIError as e:
                if e.status_code == 409:
                    print(f"\tContainer {config.name} already exists. Skipping...")
                    continue
                elif e.status_code == 404:
                    print(
                        f"\tContainer {config.name} not found. Creating new container..."
                    )
                    container = self._client.containers.run(
                        name=config.name,
                        image=config.image,
                        ports=config.port,
                        detach=config.detach_mode,
                        environment=config.env,
                    )
                    print(f"\t'{container.name}' container started")
                else:
                    print("Error starting container: ", e)
                    return False

        if self._quickstart:
            if not os.path.exists(self._quickstart_directory):
                os.makedirs(self._quickstart_directory)

            print("\nPulling Quickstart files")
            for remote_file in self._quickstart_files:
                filename = remote_file.split("/")[-1]
                print(f"\tPulling {filename}")
                local_file_path = f"{self._quickstart_directory}/{filename}"
                if not os.path.exists(local_file_path):
                    file_downloaded = self._download_file(remote_file, local_file_path)
                    if file_downloaded:
                        print(f"\t\t{filename} downloaded successfully")
                    else:
                        print(f"\t\t{filename} could not be downloaded. Skipping...")
                else:
                    print(f"\t\t{filename} already exists. Skipping...")

        print("\nFeatureform is now running!")
        print("To access the dashboard, visit http://localhost:80")

        if self._quickstart:
            print(
                f"Run jupyter notebook in the {self._quickstart_directory} directory to get started."
            )
        else:
            print(
                "To apply definition files, run `featureform apply <file.py> --host http://localhost:7878 --insecure`"
            )
        return True

    def stop(self) -> bool:
        print("Stopping containers...")
        for config in self._config:
            try:
                container = self._client.containers.get(config.name)
                if container.status == "running":
                    print(f"\tStopping {config.name} container")
                    container.stop()
            except docker.errors.NotFound:
                print(f"Container {config.name} not found. Skipping...")
                continue
            except docker.errors.APIError as e:
                print("Error stopping container: ", e)
                return False
        return True

    def _download_file(self, file, local_file_path):
        try:
            response = requests.get(file, stream=True, verify=False)

            if response.status_code == 200:
                with open(local_file_path, "wb") as local_file:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            local_file.write(chunk)
                print(f"Downloaded successfully to {local_file_path}")
            else:
                print(
                    f"""
                    Failed to download {file}. Status code: {response.status_code}\n
                    In order to download the file, you can get it directly from AWS.
                    ```
                    wget {file} -O {local_file_path}
                    ```
                    """
                )
                return False
        except Exception:
            print(
                f"""Error downloading the file\n 
                In order to download the file, you can get it directly from AWS.
                ```
                wget {file} -O {local_file_path}
                ```
                """
            )
            return False

        return True
