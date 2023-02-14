from dataclasses import dataclass


@dataclass(frozen=True)
class OrderBy:
    column_name: str
    desc: bool
    # TODO nulls first/last
