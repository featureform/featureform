import sys
import time
from typing import Type, Tuple, List

from dataclasses import dataclass
from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.text import Text

from featureform.proto.metadata_pb2_grpc import ApiStub
from featureform.resources import (
    Resource,
    Provider,
    FeatureVariant,
    OnDemandFeatureVariant,
    TrainingSetVariant,
    LabelVariant,
    SourceVariant,
)

# maximum number of dots printing when featureform apply for Running...
MAX_NUM_RUNNING_DOTS = 10
SECONDS_BETWEEN_STATUS_CHECKS = 2


def display_statuses(stub: ApiStub, resources: List[Resource], verbose=False):
    StatusDisplayer(stub, resources, verbose=verbose).display()


@dataclass
class DisplayStatus:
    resource_type: Type
    name: str
    variant: str
    status: str = "NO_STATUS"
    error: str = None

    def is_finished(self) -> bool:
        return (
            self.status == "READY"
            or self.status == "FAILED"
            or (
                self.resource_type is Provider and self.status == "NO_STATUS"
            )  # Provider is a special case
        )

    @staticmethod
    def from_resource(resource: Resource):
        variant = getattr(resource, "variant", "")
        return DisplayStatus(
            resource_type=type(resource),
            name=resource.name,
            variant=variant,
            status=resource.status,
            error=resource.error,
        )


class StatusDisplayer:
    did_error: bool = False
    RESOURCE_TYPES_TO_CHECK = {
        FeatureVariant,
        OnDemandFeatureVariant,
        TrainingSetVariant,
        LabelVariant,
        SourceVariant,
        Provider,
    }

    STATUS_TO_COLOR = {
        "READY": "green",
        "CREATED": "green",
        "PENDING": "yellow",
        "NO_STATUS": "white",
        "FAILED": "red",
    }

    def __init__(self, stub: ApiStub, resources: List[Resource], verbose=False):
        self.verbose = verbose
        filtered_resources = filter(
            lambda r: any(
                isinstance(r, resource_type)
                for resource_type in self.RESOURCE_TYPES_TO_CHECK
            ),
            resources,
        )
        self.stub = stub

        # A more intuitive way to is to store OrderedDict[Resource, DisplayStatus] but you can't hash Resource easily
        self.resource_to_status_list: List[Tuple[Resource, DisplayStatus]] = []

        for r in filtered_resources:
            self.resource_to_status_list.append((r, DisplayStatus.from_resource(r)))

    def update_display_statuses(self):
        for resource, display_status in self.resource_to_status_list:
            if resource.name == "local-mode":
                continue
            if not display_status.is_finished():
                r = resource.get(self.stub)
                display_status.status = r.status
                display_status.error = r.error
                if r.status == "FAILED":
                    self.did_error = True

    def all_statuses_finished(self) -> bool:
        return all(status.is_finished() for _, status in self.resource_to_status_list)

    def create_error_message(self):
        message = ""
        for _, status in self.resource_to_status_list:
            name = status.name
            message += f"{name}: {status.status} - {status.error}\n"
        return message

    def display(self):
        if not self.resource_to_status_list:
            return

        print()
        console = Console()
        with Live(console=console, auto_refresh=True, screen=False) as live:
            i = 0
            while True:
                self.update_display_statuses()
                finished_running = self.all_statuses_finished()

                dots = "." * (1 + i % MAX_NUM_RUNNING_DOTS)

                title = (
                    f"[green]COMPLETED[/]"
                    if finished_running
                    else f"[yellow]RUNNING{dots}[/]"
                )
                table = Table(
                    title=title,
                    title_justify="left",
                    show_header=True,
                    header_style="bold",
                    box=None,
                )

                no_table = False
                if len(self.resource_to_status_list) == 1:
                    status = self.resource_to_status_list[0][1]
                    if status.name == "local-mode":
                        no_table = True

                if not no_table:
                    table.add_column("Resource Type", width=25)
                    table.add_column("Name (Variant)", width=50, no_wrap=True)
                    table.add_column("Status", width=10)
                    table.add_column("Error", style="red")

                    for resource, status in self.resource_to_status_list:
                        error = f" {status.error}" if status.error else ""
                        if status.name == "local-mode":
                            continue
                        resource_type = status.resource_type.__name__
                        if (
                            isinstance(resource, SourceVariant)
                            and resource.is_transformation_type()
                        ):
                            resource_type = "Transformation"
                        name = status.name
                        status_text = (
                            status.status
                            if status.resource_type is not Provider
                            else "CREATED"
                        )

                        table.add_row(
                            Text(resource_type),
                            Text(f"{name} ({status.variant})"),
                            Text(status_text, style=self.STATUS_TO_COLOR[status_text]),
                            Text(error, style="red"),
                        )

                live.update(table)
                live.refresh()

                if finished_running:
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
