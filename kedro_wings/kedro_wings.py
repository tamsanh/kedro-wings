import logging
import os
from functools import reduce
from typing import Dict, Iterable, Any, Optional

from kedro.framework.context import KedroContext
from kedro.framework.hooks import hook_impl
from kedro.io import DataCatalog, AbstractDataSet
from kedro.pipeline import Pipeline

from .wing_info import (
    WingInfo,
    parse_wing_info,
)

logger = logging.getLogger("KedroWings")


class KedroWingsException(Exception):
    pass


class InvalidKedroWingsDataSet(KedroWingsException):
    pass


class MissingType(KedroWingsException):
    pass


class MissingChronoDataSetTarget(KedroWingsException):
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
        root: str = "data",
        namespaces: Iterable[str] = None,
        enabled: bool = True,
        context: Optional[KedroContext] = None,
    ):
        """
        KedroWings Hook
        :param dataset_configs: A mapping of file name extensions to the type of dataset to be created.
        :param paths: A mapping of old path names to new path names.
        :param root: The root directory to save files to. Default: data
        :param enabled: Convenience flag to enable or disable this plugin.
        :param context: Used when inside of a notebook
        """

        dataset_configs = dataset_configs or {}
        paths = paths or {}

        self._dataset_configs = {**self.DEFAULT_TYPES, **dataset_configs}
        self._paths = paths
        self._enabled = enabled
        self._root = root
        namespaces = namespaces or []
        self._namespaces = [n if not n.endswith(".") else n[:-1] for n in namespaces]

        if context:
            all_pipelines = reduce(
                lambda x, y: x + y, context.pipelines.values(), Pipeline([])
            )
            self._add_wings_to_context(all_pipelines, context)

    @staticmethod
    def _verify_config(ext: str, found_config: Dict):
        """
        Verifies a config
        """
        if "type" not in found_config:
            raise MissingType(f'Configuration for {ext} is missing its "type" key.')

    def _wing_to_dataset_config(self, wing: WingInfo) -> Dict:
        """
        Parsing a wing to make it fit with a dataset config
        """
        directory = self._paths.get(wing.directory, wing.directory)
        filepath_dir = directory
        if self._root:
            filepath_dir = os.path.join(self._root, directory)
        filepath = os.path.join(filepath_dir, wing.basename)
        found_config = self._dataset_configs[wing.extension]
        if type(found_config) is not dict:
            found_config = {"type": found_config}
        self._verify_config(wing.extension, found_config)
        dataset_config = {
            "filepath": filepath,
            **found_config,
        }
        return dataset_config

    def _create_entries(
        self,
        dataset_catalog_names: Iterable[str],
        catalog_datasets: Dict[str, AbstractDataSet],
    ):
        """
        Creates a set of catalog entries based on wing and chronocoded datasets
        """
        wing_entries = self._create_wing_entries(dataset_catalog_names)
        catalog_and_wings = {**catalog_datasets, **wing_entries}
        chrono_datasets = self._create_chronocode_entries(
            dataset_catalog_names, catalog_and_wings
        )
        return {**wing_entries, **chrono_datasets}

    def _create_wing_entries(
        self, dataset_catalog_names: Iterable[str]
    ) -> Dict[str, AbstractDataSet]:
        out = {}
        for dataset_catalog_name in dataset_catalog_names:
            if dataset_catalog_name.endswith("!"):
                continue

            wing = parse_wing_info(
                dataset_catalog_name, self._dataset_configs.keys(), self._namespaces
            )
            if wing == WingInfo():
                continue
            out[dataset_catalog_name] = AbstractDataSet.from_config(
                dataset_catalog_name, self._wing_to_dataset_config(wing)
            )
        return out

    def _create_chronocode_entries(
        self,
        dataset_catalog_names: Iterable[str],
        catalog_datasets: Dict[str, AbstractDataSet],
    ):
        out = {}

        for dataset_catalog_name in dataset_catalog_names:
            if not dataset_catalog_name.endswith("!"):
                continue
            nonchrono_name = dataset_catalog_name[:-1]
            wing = parse_wing_info(
                nonchrono_name, self._dataset_configs.keys(), self._namespaces
            )
            if wing != WingInfo():
                out[dataset_catalog_name] = AbstractDataSet.from_config(
                    dataset_catalog_name, self._wing_to_dataset_config(wing)
                )
                continue

            found_dataset = catalog_datasets.get(nonchrono_name)
            if found_dataset is None:
                raise MissingChronoDataSetTarget(dataset_catalog_name)
            out[dataset_catalog_name] = found_dataset

        return out

    _backup_attr_name = "__wings_backup_get_catalog"

    def _add_wings_to_context(self, pipeline: Pipeline, context: KedroContext):

        logger.info("KedroWings added to Context")
        if getattr(context, KedroWings._backup_attr_name, None):
            return

        all_dataset_names = set(
            [
                ds
                for node in pipeline.nodes
                for ds in [inp for inp in node.inputs] + [outp for outp in node.outputs]
            ]
        )

        setattr(context, KedroWings._backup_attr_name, context._get_catalog)

        def _generate_wings_catalog(self_context, self_catalog_entries):
            def _get_wings_catalog():
                catalog = getattr(self_context, KedroWings._backup_attr_name,)()

                existing_catalog_names = set(catalog.list())
                for catalog_name, catalog_dataset in self_catalog_entries.items():
                    if catalog_name in existing_catalog_names:
                        continue
                    catalog.add(catalog_name, catalog_dataset)
                return catalog

            return _get_wings_catalog

        all_new_entries = self._create_entries(
            all_dataset_names, context.catalog._data_sets
        )
        setattr(
            context, "_get_catalog", _generate_wings_catalog(context, all_new_entries)
        )

    @hook_impl
    def before_pipeline_run(
        self, run_params: Dict, pipeline: Pipeline, catalog: DataCatalog
    ):
        if not self._enabled:
            return

        logger.info("KedroWings Added to Catalog")
        all_dataset_names = set(
            [
                ds
                for node in pipeline.nodes
                for ds in [inp for inp in node.inputs] + [outp for outp in node.outputs]
            ]
        )

        all_new_entries = self._create_entries(all_dataset_names, catalog._data_sets)

        existing_catalog_names = set(catalog.list())
        for catalog_name, catalog_dataset in all_new_entries.items():
            if catalog_name in existing_catalog_names:
                continue
            catalog.add(catalog_name, catalog_dataset)
