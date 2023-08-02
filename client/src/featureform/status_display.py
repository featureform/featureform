import time
from typing import Type, Tuple, List

from dataclasses import dataclass
from featureform.proto.metadata_pb2_grpc import ApiStub

from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.text import Text

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


def display_statuses(stub: ApiStub, resources: List[Resource]):
    StatusDisplayer(stub, resources).display()


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

    def __init__(self, stub: ApiStub, resources: List[Resource]):
        filtered_resources = filter(
            lambda r: type(r) in self.RESOURCE_TYPES_TO_CHECK, resources
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

    def all_statuses_finished(self) -> bool:
        return all(status.is_finished() for _, status in self.resource_to_status_list)

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
                table.add_column("Resource Type", width=25)
                table.add_column("Name (Variant)", width=50, no_wrap=True)
                table.add_column("Status", width=10)
                table.add_column("Error", style="red")

                for _, status in self.resource_to_status_list:
                    error = f" {status.error}" if status.error else ""
                    resource_type = status.resource_type.__name__
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
                    break

                i += 1
                time.sleep(SECONDS_BETWEEN_STATUS_CHECKS)
