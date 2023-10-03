two_row_spacing = "{:<30} {:<25}"
three_row_spacing = "{:<30} {:<30} {:<30}"
four_row_spacing = "{:<30} {:<30} {:<30} {:<30}"
five_row_spacing = "{:<30} {:<30} {:<30} {:<30} {:<30}"
divider = "-----------------------------------------------"


def format_rows(
    format_obj,
    format_obj_2=None,
    format_obj_3=None,
    format_obj_4=None,
    format_obj_5=None,
):
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
    elif (
        format_obj_2 is not None
        and format_obj_3 is not None
        and format_obj_4 is not None
        and format_obj_5 is None
    ):
        print(
            four_row_spacing.format(
                format_obj, format_obj_2, format_obj_3, format_obj_4
            )
        )
    else:
        print(
            five_row_spacing.format(
                format_obj, format_obj_2, format_obj_3, format_obj_4, format_obj_5
            )
        )


def format_pg(s=""):
    print(divider)
    print(s)


def progress_bar(total, current, prefix="", suffix="", length=30, fill="#"):
    import sys

    if total == 0:
        return

    percent = current / total
    filled_length = int(length * percent)
    bar = fill * filled_length + "-" * (length - filled_length)
    sys.stdout.write("\r{} |{}| {}% {}".format(prefix, bar, int(percent * 100), suffix))
    sys.stdout.flush()
