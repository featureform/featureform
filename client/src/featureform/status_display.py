#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import sys
import time
from typing import List

from dataclasses import dataclass
from rich.console import Console, Group
from rich.live import Live
from rich.table import Table
from rich.text import Text

from featureform.grpc_client import GrpcClient
from featureform.resources import (
    ErrorInfo,
    FeatureVariant,
    LabelVariant,
    OnDemandFeatureVariant,
    Provider,
    Resource,
    ResourceState,
    SourceVariant,
    StreamFeature,
    StreamLabel,
    TrainingSetVariant,
)
from .enums import ResourceType, ResourceStatus

# maximum number of dots printing when featureform apply for Running...
MAX_NUM_RUNNING_DOTS = 10
SECONDS_BETWEEN_STATUS_CHECKS = 2
MAX_COMPLETED_TICKS = 1
NUM_DISPLAY_ROWS = 25

READY = ResourceStatus.READY.value
CREATED = ResourceStatus.CREATED.value
PENDING = ResourceStatus.PENDING.value
NO_STATUS = ResourceStatus.NO_STATUS.value
FAILED = ResourceStatus.FAILED.value
RUNNING = ResourceStatus.RUNNING.value


def display_statuses(
    grpc_client: GrpcClient, resources: List[Resource], host, verbose=False
):
    StatusDisplayer(grpc_client, resources, verbose=verbose).display(host)


@dataclass
class DisplayStatus:
    resource_type: ResourceType
    name: str
    variant: str
    status: str = NO_STATUS
    error: str = ""
    has_health_check: bool = False

    def is_finished(self) -> bool:
        return (
            self.status == READY
            or self.status == FAILED
            or (
                self.resource_type is ResourceType.PROVIDER
                and self.status == NO_STATUS
                and not self.has_health_check
            )  # Provider is a special case
        )

    @classmethod
    def from_resource(self, resource: Resource):
        variant = getattr(resource, "variant", "")
        return DisplayStatus(
            resource_type=resource.get_resource_type(),
            name=resource.name,
            variant=variant,
            status=resource.status,
            error=resource.error,
            has_health_check=bool(getattr(resource, "has_health_check", False)),
        )


class ResourceTableRow:
    def __init__(self, resource: Resource, status: DisplayStatus):
        self._status = status
        self._time_ticks = 0
        if not resource:
            raise ValueError("Resource cannot be empty")
        self.name = resource.name
        self.resource = resource

    def update_status(self, new_status: ResourceStatus = None, error=None):
        if error is None and new_status is None:
            raise ValueError(f"No updates provided to status for {self.name}")
        if error is not None:
            self._status.error = error
        if new_status is not None:
            self._status.status = new_status

    def get_status(self) -> DisplayStatus:
        return self._status

    # One time tick corresponds to an iteration of the "while True" loop of display()
    def update_time_tick(self):
        self._time_ticks += 1

    def get_time_ticks(self):
        return self._time_ticks

    def get_status_resourcetype(self):
        return (
            ResourceType.TRANSFORMATION
            if (
                isinstance(self.resource, SourceVariant)
                and self.resource.is_transformation_type()
            )
            else self._status.resource_type
        )

    def get_status_string(self):
        return (
            self._status.status
            if self._status.resource_type is not ResourceType.PROVIDER
            or self._status.has_health_check
            else CREATED
        )


class StatusDisplayer:
    did_error: bool = False
    RESOURCE_TYPES_TO_CHECK = {
        FeatureVariant,
        OnDemandFeatureVariant,
        TrainingSetVariant,
        LabelVariant,
        SourceVariant,
        StreamFeature,
        StreamLabel,
        Provider,
    }

    STATUS_TO_COLOR = {
        READY: "green",
        CREATED: "green",
        PENDING: "white",
        NO_STATUS: "white",
        FAILED: "red",
        RUNNING: "yellow",
    }

    def __init__(
        self, grpc_client: GrpcClient, resources: List[Resource], verbose=False
    ):
        self.verbose = verbose
        filtered_resources = filter(
            lambda r: any(
                isinstance(r, resource_type)
                for resource_type in self.RESOURCE_TYPES_TO_CHECK
            ),
            resources,
        )
        self.grpc_client = grpc_client
        self.success_list: List[ResourceTableRow] = []
        self.failed_list: List[ResourceTableRow] = []
        self.display_table_list: List[ResourceTableRow] = []
        self.unprocessed_resources: List[ResourceTableRow] = []

        for r in filtered_resources:
            self.unprocessed_resources.append(
                ResourceTableRow(r, DisplayStatus.from_resource(r))
            )

    def update_resource_status(self, resource_table_row: ResourceTableRow):
        if not resource_table_row.get_status().is_finished():
            res = resource_table_row.resource.get(self.grpc_client)
            server_status = res.server_status
            resource_table_row.update_status(new_status=server_status.status)

            if server_status.error_info is not None:
                resource_table_row.update_status(
                    error=self._format_error_info(server_status.error_info)
                )
            else:
                resource_table_row.update_status(error=res.error)

    @staticmethod
    def _format_error_info(error_info: ErrorInfo):
        message = error_info.message
        reason = error_info.reason
        metadata = error_info.metadata

        formatted_metadata = "\n".join(
            [f"{key}: {value}" for key, value in metadata.items()]
        )
        return f"{reason}: {message}\n{formatted_metadata}"

    def all_statuses_finished(self) -> bool:
        return (
            len(self.unprocessed_resources) == 0 and len(self.display_table_list) == 0
        )

    def create_error_message(self):
        message = ""
        for resource_table_row in self.failed_list:
            name = resource_table_row.name
            message += f"{name}: {resource_table_row.get_status_string()} - {resource_table_row.get_status().error}\n"
        return message

    def display(self, host):
        if not self.unprocessed_resources:
            print("No resources to display")
            return

        console = Console(record=True)
        self.setup_display_resources()
        with Live(console=console, auto_refresh=False, screen=False) as live:
            # Used for updating loading dots based on the iteration number
            i = 0
            while True:
                self.update_display_data()
                display_table = self.create_display_table(i)
                live.update(display_table, refresh=True)

                if self.all_statuses_finished():
                    success_table = self.create_success_table(host)
                    failure_table = self.create_failure_table()
                    # Create the group only if there's at least one table
                    tables = [
                        table for table in [success_table, failure_table] if table
                    ]

                    if tables:
                        table_group = Group(*tables)  # Unpack the tables list
                        live.update(table_group, refresh=True)

                    # This block is used for testing
                    # Tests check for both stderr and an exception
                    # If we don't throw an exception, then tests will pass even when things fail to register
                    # We also print all the error messages because the table does not get saved when
                    # capturing stdout/stderr
                    if self.verbose and self.did_error:
                        statuses = self.create_error_message()
                        sys.tracebacklimit = 0
                        raise Exception("Some resources failed to create\n" + statuses)
                    break

                i += 1
                time.sleep(SECONDS_BETWEEN_STATUS_CHECKS)

    def setup_display_resources(self):
        # Adding 25 rows from unprocessed_resources to display_table_list
        self.display_table_list = self.unprocessed_resources[:NUM_DISPLAY_ROWS]
        del self.unprocessed_resources[:NUM_DISPLAY_ROWS]

    def update_display_data(self):
        self._process_existing_display_rows()
        self._fill_display_rows_from_unprocessed_list()

    def _process_existing_display_rows(self):
        # We need to make a copy of the list because we are modifying it in the loop
        initial_display_table_list = self.display_table_list[:]
        for resource_table_row in initial_display_table_list:
            self.update_resource_status(resource_table_row)
            current_status = resource_table_row.get_status_string()
            if (
                current_status in {CREATED, READY}
                and resource_table_row not in self.success_list
            ):
                self.success_list.append(resource_table_row)
            elif (
                current_status == FAILED and resource_table_row not in self.failed_list
            ):
                self.failed_list.append(resource_table_row)

            # If the resource is complete, we should stop displaying after MAX_COMPLETED_TICKS
            if current_status not in {
                PENDING,
                RUNNING,
                NO_STATUS,
            }:
                resource_table_row.update_time_tick()
                if resource_table_row.get_time_ticks() > MAX_COMPLETED_TICKS:
                    self.display_table_list.remove(resource_table_row)

    # Fill the display_table_list with resources from unprocessed_resources
    def _fill_display_rows_from_unprocessed_list(self):
        while (
            len(self.display_table_list) < NUM_DISPLAY_ROWS
            and len(self.unprocessed_resources) > 0
        ):
            next_row = self.unprocessed_resources.pop(0)
            self.update_resource_status(next_row)
            self.add_resource_to_display_state(next_row)

    def add_resource_to_display_state(self, resource: ResourceTableRow):
        if (
            resource in self.success_list
            or resource in self.failed_list
            or resource in self.display_table_list
        ):
            raise ValueError("Resource should not be in success or failed list")
        current_status = resource.get_status_string()
        if current_status in {CREATED, READY}:
            self.success_list.append(resource)
        elif current_status == FAILED:
            self.failed_list.append(resource)
        elif current_status in {PENDING, NO_STATUS, RUNNING}:
            self.display_table_list.append(resource)
        else:
            raise ValueError(f"Invalid status {current_status}")

    def create_display_table(self, i):
        dots = "." * (1 + i % MAX_NUM_RUNNING_DOTS)
        title = f"[yellow]RUNNING{dots}[/]"
        table = self._create_table(title, "Status", True, None)
        for resource_table_row in self.display_table_list:
            status = resource_table_row.get_status()
            resource_type = resource_table_row.get_status_resourcetype()
            name = resource_table_row.name
            status_text = resource_table_row.get_status_string()
            table.add_row(
                Text(resource_type.to_string()),
                Text(f"{name} ({status.variant})"),
                Text(status_text, style=self.STATUS_TO_COLOR[status_text]),
            )
        return table

    def _create_table(
        self, table_name, other_column, other_col_nowrap, other_col_style
    ):
        table = Table(
            title=table_name,
            title_justify="left",
            expand=True,
            show_header=True,
            header_style="bold",
            box=None,
        )
        table.add_column("Resource Type", width=15, no_wrap=True)
        table.add_column("Name (Variant)", width=35)
        table.add_column(
            other_column, width=50, style=other_col_style, no_wrap=other_col_nowrap
        )
        return table

    def create_success_table(self, host):
        if len(self.success_list) == 0:
            return None
        resource_state = ResourceState()
        table = self._create_table("Completed Resources", "Dashboard Links", True, None)
        for resource_table_row in self.success_list:
            status = resource_table_row.get_status()
            resource_type = resource_table_row.get_status_resourcetype()
            name = resource_table_row.name
            url = resource_state.build_dashboard_url(
                host, resource_type, name, status.variant
            )
            table.add_row(
                Text(resource_type.to_string()),
                Text(f"{name} ({status.variant})"),
                Text(url, style="link", overflow="crop"),
            )
        return table

    def create_failure_table(self):
        if len(self.failed_list) == 0:
            return None
        table = self._create_table("Failed Resources", "Error", False, "red")
        for resource_table_row in self.failed_list:
            status = resource_table_row.get_status()
            resource_type = resource_table_row.get_status_resourcetype()
            name = resource_table_row.name
            error = f" {status.error}" if status.error else ""
            table.add_row(
                Text(resource_type.to_string()),
                Text(f"{name} ({status.variant})"),
                Text(error, style="red", overflow="fold"),
            )
        return table
