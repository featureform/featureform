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