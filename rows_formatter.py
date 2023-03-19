import csv as csv
import io

from tabulate import tabulate

import constants


def format(rows, column_names, format_type):
    match format_type:
        case 'csv':
            return csv_(rows, column_names)
        case 'tabulate':
            return tabulate_(rows, column_names)


def tabulate_(rows, column_names):
    return tabulate(rows, headers=column_names, tablefmt=constants.TABULATE_FORMAT)


def csv_(rows, column_names):
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(column_names)
    writer.writerows(rows)
    return output.getvalue()