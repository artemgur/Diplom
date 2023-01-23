import csv as csv_
import io

from tabulate import tabulate

import constants


def table(rows, column_names):
    return tabulate(rows, headers=column_names, tablefmt=constants.TABULATE_FORMAT)


def csv(rows, column_names):
    output = io.StringIO()
    writer = csv_.writer(output)
    writer.writerow(column_names)
    writer.writerows(rows)
    return output.getvalue()