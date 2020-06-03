import os

from kedro_wings import KedroWings


def test_create_catalog_entries():
    wings = KedroWings()

    catalog_name = '01_raw/data.csv'
    catalog_entry = wings._create_catalog_entries([catalog_name])[catalog_name]

    from kedro.extras.datasets.pandas import CSVDataSet
    assert isinstance(catalog_entry, CSVDataSet)
    assert str(catalog_entry._filepath) == os.path.join('data', catalog_name)

    catalog_name = '02_intermediate/data.png'
    catalog_entry = wings._create_catalog_entries([catalog_name])[catalog_name]

    from kedro.extras.datasets.pillow import ImageDataSet
    assert isinstance(catalog_entry, ImageDataSet)
    assert str(catalog_entry._filepath) == os.path.join('data', catalog_name)

    catalog_name = '02_intermediate/data.yml'
    catalog_entry = wings._create_catalog_entries([catalog_name])[catalog_name]

    from kedro.extras.datasets.yaml import YAMLDataSet
    assert isinstance(catalog_entry, YAMLDataSet)
    assert str(catalog_entry._filepath) == os.path.join('data', catalog_name)

    catalog_name = '02_intermediate/data.xls'
    catalog_entry = wings._create_catalog_entries([catalog_name])[catalog_name]

    from kedro.extras.datasets.pandas import ExcelDataSet
    assert isinstance(catalog_entry, ExcelDataSet)
    assert str(catalog_entry._filepath) == os.path.join('data', catalog_name)

    catalog_name = '02_intermediate/data.pkl'
    catalog_entry = wings._create_catalog_entries([catalog_name])[catalog_name]

    from kedro.extras.datasets.pickle import PickleDataSet
    assert isinstance(catalog_entry, PickleDataSet)
    assert str(catalog_entry._filepath) == os.path.join('data', catalog_name)
