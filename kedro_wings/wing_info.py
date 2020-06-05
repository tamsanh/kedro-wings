import os
from typing import NamedTuple, Set


class WingInfo(NamedTuple):
    directory: str = ""
    name: str = ""
    extension: str = ""
    basename: str = ""


def parse_wing_info(dataset_catalog_name: str, valid_extensions: Set[str]) -> WingInfo:
    directory = os.path.dirname(dataset_catalog_name)
    basename = os.path.basename(dataset_catalog_name)
    name, ext = os.path.splitext(basename)

    if ext not in valid_extensions:
        return WingInfo()

    return WingInfo(directory=directory, name=name, extension=ext, basename=basename)
