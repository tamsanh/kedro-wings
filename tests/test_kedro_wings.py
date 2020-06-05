import os

import pytest

from kedro_wings import KedroWings


def test_wing_info_to_config():
    from kedro_wings.wing_info import WingInfo
    from kedro_wings.kedro_wings import MissingType

    from kedro.extras.datasets import pandas

    datasets = {
        '.custom': pandas.CSVDataSet,
        '.missing_type': {'invalid': 'dataset'}
    }

    wings = KedroWings(datasets)
    info = WingInfo(directory='dir', name='data', extension='.custom', basename='data.custom')
    config = wings._wing_to_dataset_config(info)
    assert config == {'type': pandas.CSVDataSet, 'filepath': 'data/dir/data.custom'}

    with pytest.raises(MissingType):
        info = WingInfo(directory='dir', name='data', extension='.missing_type', basename='data.missing_type')
        wings._wing_to_dataset_config(info)


def test_kedro_wings_and_context():
    from kedro.pipeline import Pipeline, node
    from kedro.io import DataCatalog

    class FakeContext:
        def _get_catalog(self):
            return DataCatalog()

        @property
        def pipelines(self):
            return {'hi': Pipeline([node(lambda x: x, inputs='01_raw/data.csv', outputs='02_intermediate/data.csv')])}

        @property
        def catalog(self):
            return self._get_catalog()

    mock_context = FakeContext()

    KedroWings(context=mock_context)
    assert set(mock_context.catalog.list()) == {'01_raw/data.csv', '02_intermediate/data.csv'}


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
