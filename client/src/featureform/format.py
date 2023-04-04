import time
from typing import Type, List

from dataclasses import dataclass
from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.text import Text

two_row_spacing = "{:<30} {:<25}"
three_row_spacing = "{:<30} {:<30} {:<30}"
four_row_spacing = "{:<30} {:<30} {:<30} {:<30}"
five_row_spacing = "{:<30} {:<30} {:<30} {:<30} {:<30}"
divider = "-----------------------------------------------"


def format_rows(format_obj, format_obj_2=None, format_obj_3=None, format_obj_4=None, format_obj_5=None):
    # Base case for when `format_obj` is a string
    if format_obj_2 is None and type(format_obj) == str:
        print(format_obj)
    elif format_obj_2 is None:
        for s in format_obj:
            format_rows(*s)
    elif format_obj_2 is not None and format_obj_3 is None:
        print(two_row_spacing.format(format_obj, format_obj_2))
    elif format_obj_2 is not None and format_obj_3 is not None and format_obj_4 is None:
        print(three_row_spacing.format(format_obj, format_obj_2, format_obj_3))
    elif format_obj_2 is not None and format_obj_3 is not None and format_obj_4 is not None and format_obj_5 is None:
        print(four_row_spacing.format(format_obj, format_obj_2, format_obj_3, format_obj_4))
    else:
        print(five_row_spacing.format(format_obj, format_obj_2, format_obj_3, format_obj_4, format_obj_5))


def format_pg(s=""):
    print(divider)
    print(s)


def display_statuses(stub, resources):
    from featureform.resources import Feature, OnDemandFeature, TrainingSet, Label, Source, Provider

    @dataclass
    class Status:
        resource_type: Type
        name: str
        variant: str
        status: str
        error: str

    def get_statuses() -> List[Status]:
        statuses = []
        resources_to_check = {Feature, OnDemandFeature, TrainingSet, Label, Source, Provider}
        filtered_resources = filter(lambda r: type(r) in resources_to_check, resources)
        for r in filtered_resources:
            if r.name == "local-mode":
                continue
            resource = r.get(stub)
            variant = getattr(resource, "variant", "")
            status = Status(
                resource_type=type(r),
                name=resource.name,
                variant=variant,
                status=resource.status,
                error=resource.error,
            )
            statuses.append(status)
        return statuses

    def is_finished(statuses):
        """
        Returns True if all statuses are either READY or FAILED
        """
        return all(
            s.status == "READY"
            or s.status == "FAILED"
            or (s.resource_type is Provider and s.status == "NO_STATUS")
            for s in statuses
        )

    print()
    console = Console()
    with Live(console=console, auto_refresh=True, screen=False) as live:
        while True:
            statuses = get_statuses()
            finished_running = is_finished(statuses)

            title = (
                f"[green]COMPLETED[/]" if finished_running else f"[yellow]RUNNING...[/]"
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

            for status in statuses:
                error = f" {status.error}" if status.error else ""
                resource_type = status.resource_type.__name__
                name = status.name
                status_text = (
                    status.status if status.resource_type is not Provider else "CREATED"
                )

                status_to_color = {
                    "READY": "green",
                    "CREATED": "green",
                    "PENDING": "yellow",
                    "NO_STATUS": "white",
                    "FAILED": "red",
                }

                style = status_to_color[status_text]
                table.add_row(
                    Text(resource_type),
                    Text(f"{name} ({status.variant})"),
                    Text(status_text, style=status_to_color[status_text]),
                    Text(error, style="red")
                )

            live.update(table)
            live.refresh()

            if finished_running:
                break

            time.sleep(1)
