# Kedro Wings

As Seen on DataEngineerOne:  
*[Kedro Wings: It's almost too easy to write pipelines this way.](https://www.youtube.com/watch?v=p4ELo1tqbYY)*

<p align="center">
  <img width="255" src="https://github.com/tamsanh/kedro-wings/blob/master/images/kedro-wings.png">
</p>

Give your next kedro project Wings! The perfect plugin for brand new pipelines, and new kedro users.
This plugin enables easy and fast creation of datasets so that you can get straight into coding your pipelines.

## Quick Start Usage Example: Iris Example

The following example is a recreation of the iris example pipeline.

Kedro Wings enables super fast creation of pipelines by taking care of all the catalog work for you.
Catalog entries are automatically created by parsing the values for your nodes' inputs and outputs.

This pipeline automatically creates a dataset that reads from the `iris.csv` and then it creates 12 more datasets, corresponding to the outputs and inputs of the other datasets.

```python
wing_example = Pipeline([
    node(
        split_data,
        inputs=['01_raw/iris.csv', 'params:example_test_data_ratio'],
        outputs=dict(
            train_x="02_intermediate/example_train_x.csv",
            train_y="02_intermediate/example_train_y.csv",
            test_x="02_intermediate/example_test_x.csv",
            test_y="02_intermediate/example_test_y.csv")
        ),
    node(
        train_model,
        ["02_intermediate/example_train_x.csv", "02_intermediate/example_train_y.csv", "parameters"],
        outputs="06_models/example_model.pkl",
    ),
    node(
        predict,
        inputs=dict(
            model="06_models/example_model.pkl",
            test_x="02_intermediate/example_test_x.csv"
        ),
        outputs="07_model_output/example_predictions.pkl",
    ),
    node(
        report_accuracy,
        inputs=["07_model_output/example_predictions.pkl", "02_intermediate/example_test_y.csv"],
        None
    ),
])
```


## Installation

Kedro Wings is available on pypi, and is installed with [kedro hooks](https://kedro.readthedocs.io/en/latest/04_user_guide/15_hooks.html).



``` console
pip install kedro-wings
```


### Setup with Kedro Pipeline

Simply add a `KedroWings` instance to the `ProjectContext` `hooks` tuple.

```python
from kedro_wings import KedroWings


class ProjectContext(KedroContext):
    hooks = (
        KedroWings(),
    )
```

### Setup with Jupyter Notebook

Simply pass the kedro context into `KedroWings`, and it will automatically add all catalog entries from all available pipelines.

```python
# Load the context if not using a kedro jupyter notebook
from kedro.framework.context import load_context
context = load_context('./')

# Pass the context into KedroWings
from kedro_wings import KedroWings
KedroWings(context=context)

# context catalog now has all wings datasets available.
context.catalog.list()
```

## Usage

### Catalog Creation

Catalog entries are created using dataset input and output strings. The API is simple:

```python
inputs="[PATH]/[NAME].[EXT]"
```

The `PATH` portion determines the directory where a file will be saved.
The `NAME` portion determines the final output name of the file to be saved.
The `EXT`  portion determines the dataset used to save and load that particular data.


##### Ex: Creating an iris.csv reader
```python
node(split_data, inputs='01_raw/iris.csv', outputs='split_data_output')
```

This will create a `pandas.CSVDataSet` pointing at the `01_raw/iris.csv` file.


##### Ex: Overwrite a Kedro Wing dataset using `catalog.yml`
```python
# pipeline.py
node(split_data, inputs='01_raw/iris.csv', outputs='split_data_output')
```

```yaml
# catalog.yml
01_raw/iris.csv':
    type: pandas.CSVDataSet
    filepath: data/01_raw/iris.csv
```

If a catalog entry already exists inside of `catalog.yml`, with a name that matches the wing catalog name,
KedroWings will NOT create that catalog, and will instead defer to the `catalog.yml` entry.


#### Default Datasets

The following are the datasets available by default.

```python
default_dataset_configs={
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
```

### Configuration

Kedro Wings supports configuration on instantiation of the hook.

```
KedroWings(dataset_configs, paths, root, enabled)
```

#### dataset_configs
```
:param dataset_configs: A mapping of file name extensions to the type of dataset to be created.

```

This allows the default dataset configurations to be overridden.
This also allows the default extension to dataset mapping to be overridden or extended for other datasets.

##### Ex: Make default csv files use pipes as separators

```python
KedroWings(dataset_configs={
    '.csv': {'type': 'pandas.CSVDataSet', 'sep': '|'},
})
```

##### Ex: Use dataset types directly

```python
from kedro.extras.dataset import pandas
KedroWings(dataset_configs={
    '.csv': pandas.CSVDataSet,
})
```


#### paths

This allows specified paths to be remapped

```
:param paths: A mapping of old path names to new path names.
```

##### Ex: Moving data from 06_models to a new_models folder

```python
KedroWings(paths={
    '06_models': 'new_models',
})
```

#### root
This setting is prepended to any paths parsed. This is useful if the dataset supports `fsspec`.

```
:param root: The root directory to save files to. Default: data
```

##### Ex: Saving data to s3 instead of the local directory.

``` python
KedroWings(root='s3a://my-bucket/kedro-data')
```


#### enabled
This setting allows easy enabling and disabling of the plugin.

```
:param enabled: Convenience flag to enable or disable this plugin.
```

##### Ex: Use an environment variable to enable or disable wings

``` python
KedroWings(enabled=os.getenv('ENABLE_WINGS'))
```

