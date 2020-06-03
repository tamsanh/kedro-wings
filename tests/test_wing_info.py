import pytest

from kedro_wings.wing_info import parse_wing_info, WingInfo

__version__ = "0.1.0"


@pytest.fixture
def valid_catalog_names():
    return [
        '01_raw/sample_data.csv',
        '02_intermediate/sample_data.txt',
        '03_primary/sample_data.png',
    ]


@pytest.fixture
def valid_extensions():
    return {'.csv', '.txt', '.png'}


@pytest.fixture
def valid_quick_datasets():
    return [
        WingInfo('01_raw', 'sample_data', '.csv', 'sample_data.csv'),
        WingInfo('02_intermediate', 'sample_data', '.txt', 'sample_data.txt'),
        WingInfo('03_primary', 'sample_data', '.png', 'sample_data.png'),
    ]


def test_parse_valid_entries(valid_catalog_names, valid_extensions, valid_quick_datasets):
    for valid_catalog_name, valid_quick_dataset in zip(valid_catalog_names, valid_quick_datasets):
        parsed_quick_dataset = parse_wing_info(valid_catalog_name, valid_extensions)
        assert parsed_quick_dataset == valid_quick_dataset


def test_parse_invalid_entries(valid_catalog_names):
    for valid_catalog_name in valid_catalog_names:
        assert WingInfo() == parse_wing_info(valid_catalog_name, set())
