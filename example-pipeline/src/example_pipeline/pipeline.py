# Copyright 2020 QuantumBlack Visual Analytics Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
# NONINFRINGEMENT. IN NO EVENT WILL THE LICENSOR OR OTHER CONTRIBUTORS
# BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF, OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# The QuantumBlack Visual Analytics Limited ("QuantumBlack") name and logo
# (either separately or in combination, "QuantumBlack Trademarks") are
# trademarks of QuantumBlack. The License does not grant you any right or
# license to the QuantumBlack Trademarks. You may not use the QuantumBlack
# Trademarks or any confusingly similar mark as a trademark for your product,
# or use the QuantumBlack Trademarks in any other manner that might cause
# confusion in the marketplace, including but not limited to in advertising,
# on websites, or on software.
#
# See the License for the specific language governing permissions and
# limitations under the License.

"""Construction of the master pipeline.
"""

from typing import Dict

from kedro.pipeline import Pipeline, node

###########################################################################
# Here you can find an example pipeline, made of two modular pipelines.
#
# Delete this when you start working on your own Kedro project as
# well as pipelines/data_science AND pipelines/data_engineering
# -------------------------------------------------------------------------
from example_pipeline.pipelines.data_engineering.nodes import split_data
from example_pipeline.pipelines.data_science.nodes import (
    train_model,
    predict,
    report_accuracy,
)


def create_pipelines(**kwargs) -> Dict[str, Pipeline]:
    """Create the project's pipeline.

    Args:
        kwargs: Ignore any additional arguments added in the future.

    Returns:
        A mapping from a pipeline name to a ``Pipeline`` object.

    """
    wing_example = Pipeline([
        node(
            split_data,
            ['01_raw/iris.csv', 'params:example_test_data_ratio'],
            dict(
                train_x="02_intermediate/example_train_x.csv",
                train_y="02_intermediate/example_train_y.csv",
                test_x="02_intermediate/example_test_x.csv",
                test_y="02_intermediate/example_test_y.csv",
            )
        ),
        node(
            train_model,
            ["02_intermediate/example_train_x.csv", "02_intermediate/example_train_y.csv", "parameters"],
            "06_models/example_model.pkl",
        ),
        node(
            predict,
            dict(model="06_models/example_model.pkl", test_x="02_intermediate/example_test_x.csv"),
            "07_model_output/example_predictions.pkl",
        ),
        node(report_accuracy, ["07_model_output/example_predictions.pkl", "02_intermediate/example_test_y.csv"], None),
    ])

    return {"__default__": wing_example}
