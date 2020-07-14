import os
from typing import NamedTuple, Iterable


class WingInfo(NamedTuple):
    directory: str = ""
    name: str = ""
    extension: str = ""
    basename: str = ""


def parse_wing_info(
    dataset_catalog_name: str,
    valid_extensions: Iterable[str],
    namespaces: Iterable[str] = None,
) -> WingInfo:

    for valid_extension in sorted(
        sorted(valid_extensions, key=lambda x: len(x), reverse=True),
        key=lambda x: len(x.split(".")),
        reverse=True,
    ):

        if dataset_catalog_name.endswith(valid_extension):
            break
    else:
        return WingInfo()

    cleaned_name = dataset_catalog_name[: -len(valid_extension)]

    directory = os.path.dirname(cleaned_name)
    name = os.path.basename(cleaned_name)
    ext = valid_extension

    basename = f"{name}{ext}"

    return WingInfo(directory=directory, name=name, extension=ext, basename=basename)
