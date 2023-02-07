from typing import List
from rich.console import Console
from rich.table import Table
from .resources import Resource


two_row_spacing = "{:<30} {:<25}"
three_row_spacing = "{:<30} {:<30} {:<30}"
four_row_spacing = "{:<30} {:<30} {:<30} {:<30}"
divider = "-----------------------------------------------"

def format_rows(format_obj, format_obj_2=None, format_obj_3=None, format_obj_4=None):
    if format_obj_2 is None:
        for s in format_obj:
            format_rows(*s)
    elif format_obj_2 is not None and format_obj_3 is None:
        print(two_row_spacing.format(format_obj, format_obj_2))
    elif format_obj_2 is not None and format_obj_3 is not None and format_obj_4 is None:
        print(three_row_spacing.format(format_obj, format_obj_2, format_obj_3))
    else:
        print(four_row_spacing.format(format_obj, format_obj_2, format_obj_3, format_obj_4))

def format_pg(s=""):
    print(divider)
    print(s)


class CliFormatter:

    def __init__(self) -> None:
        self.__console = Console()

    def print_to_table(self, resources: List[Resource]):
        if len(resources) == 0:
            return

        first_resource = resources[0]

        table = Table(f"{first_resource.type().upper()} NAME", "TYPE", "STATUS")

        for resource in resources:
            status = resource.get_status() if self._has_status(resource) else ""

            table.add_row(resource.name, resource.type(), status)

        self.__console.print(table)

    def _has_status(self, resource: Resource):
        return hasattr(resource, 'status') and callable(getattr(resource, 'status'))


CliFormatter = CliFormatter()