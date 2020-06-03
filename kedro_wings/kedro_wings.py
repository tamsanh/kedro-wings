import logging
import os
from typing import Dict, Iterable, Any

from kedro.framework.hooks import hook_impl
from kedro.io import DataCatalog, AbstractDataSet
from kedro.pipeline import Pipeline

from .wing_info import (
    WingInfo,
    parse_wing_info,
)

logger = logging.getLogger("KedroQuickPipe")


class KedroWingsException(Exception):
    pass


class InvalidKedroWingsDataSet(KedroWingsException):
    pass


class MissingType(KedroWingsException):
    pass


class KedroWings:
    DEFAULT_TYPES = {
        ".csv": {"type": "pandas.CSVDataSet"},
        ".yml": {"type": "yaml.YAMLDataSet"},
        ".yaml": {"type": "yaml.YAMLDataSet"},
        ".xls": {"type": "pandas.ExcelDataSet"},
        ".txt": {"type": "text.TextDataSet"},
        ".png": {"type": "pillow.ImageDataSet"},
        ".jpg": {"type": "pillow.ImageDataSet"},
        ".jpeg": {"type": "pillow.ImageDataSet"},
        ".img": {"type": "pillow.ImageDataSet"},
        ".pkl": {"type": "pickle.PickleDataSet"},
        ".parquet": {"type": "pandas.ParquetDataSet"},
    }

    def __init__(
        self,
        dataset_configs: Dict[str, Any] = None,
        paths: Dict[str, str] = None,
        root: str = None,
        enabled: bool = True,
    ):
        """
        KedroWings Hook
        :param dataset_configs: A mapping of file name extensions to the type of dataset to be created.
        :param paths: A mapping of old path names to new path names.
        :param root: The root directory to save files to. Default: data
        :param enabled: Convenience flag to enable or disable this plugin.
        """

        dataset_configs = dataset_configs or {}
        paths = paths or {}
        root = root or "data"

        self._dataset_configs = {**self.DEFAULT_TYPES, **dataset_configs}
        self._paths = paths
        self._enabled = enabled
        self._root = root

    @staticmethod
    def _verify_config(ext: str, found_config: Dict):
        if 'type' not in found_config:
            raise MissingType(f'Configuration for {ext} is missing its "type" key.')

    def _wing_to_dataset_config(self, wing: WingInfo) -> Dict:
        filepath_dir = os.path.join(self._root, wing.directory)
        filepath = os.path.join(filepath_dir, wing.basename)
        found_config = self._dataset_configs[wing.extension]
        if type(found_config) is not dict:
            found_config = {'type': found_config}
        self._verify_config(wing.extension, found_config)
        dataset_config = {
            "filepath": filepath,
            **found_config,
        }
        return dataset_config

    def _create_catalog_entries(
        self, dataset_catalog_names: Iterable[str]
    ) -> Dict[str, AbstractDataSet]:
        out = {}
        for dataset_catalog_name in dataset_catalog_names:
            wing = parse_wing_info(
                dataset_catalog_name, set(self._dataset_configs.keys())
            )
            if wing == WingInfo():
                continue
            out[dataset_catalog_name] = AbstractDataSet.from_config(
                dataset_catalog_name, self._wing_to_dataset_config(wing)
            )
        return out

    @hook_impl
    def before_pipeline_run(
        self, run_params: Dict, pipeline: Pipeline, catalog: DataCatalog
    ):
        if not self._enabled:
            return

        logger.info("KedroWings is Enabled")
        all_dataset_names = set(
            [
                ds
                for node in pipeline.nodes
                for ds in [inp for inp in node.inputs] + [outp for outp in node.outputs]
            ]
        )

        catalog_entries = self._create_catalog_entries(all_dataset_names)

        for catalog_name, catalog_dataset in catalog_entries.items():
            catalog.add(catalog_name, catalog_dataset)
