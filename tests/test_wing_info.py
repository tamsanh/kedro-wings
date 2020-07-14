import os

import pytest

from kedro_wings.wing_info import parse_wing_info, WingInfo

__version__ = "0.1.0"


@pytest.fixture
def valid_catalog_names():
    return [
        "01_raw/sample_data.csv",
        "02_intermediate/sample_data.txt",
        "03_primary/sample_data.png",
    ]


@pytest.fixture
def valid_extensions():
    return {".csv", ".txt", ".png"}


@pytest.fixture
def valid_wings_datasets():
    return [
        WingInfo("01_raw", "sample_data", ".csv", "sample_data.csv"),
        WingInfo("02_intermediate", "sample_data", ".txt", "sample_data.txt"),
        WingInfo("03_primary", "sample_data", ".png", "sample_data.png"),
    ]


def test_parse_valid_entries(
    valid_catalog_names, valid_extensions, valid_wings_datasets
):
    for valid_catalog_name, valid_wings_dataset in zip(
        valid_catalog_names, valid_wings_datasets
    ):
        parsed_wings_dataset = parse_wing_info(valid_catalog_name, valid_extensions)
        assert parsed_wings_dataset == valid_wings_dataset


def test_parse_invalid_entries(valid_catalog_names):
    for valid_catalog_name in valid_catalog_names:
        assert WingInfo() == parse_wing_info(valid_catalog_name, set())


def test_prioritize_multiple_extensions():
    extensions = {".html", ".profile.html", ".csv", ".true.csv"}
    valid_catalog_names = [
        "01_raw/test.html",
        "01_raw/test.profile.html",
        "01_raw/test.true.csv",
    ]
    for valid_catalog_name in valid_catalog_names:
        parsed_wing = parse_wing_info(valid_catalog_name, extensions)
        assert (
            os.path.join(parsed_wing.directory, parsed_wing.basename)
            == valid_catalog_name
        )
