from great_expectations.expectations.expectation import Expectation
from great_expectations.validator.metric_configuration import MetricConfiguration
from great_expectations.render import RenderedStringTemplateContent
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from typing import Dict
from great_expectations.execution_engine import ExecutionEngine
class ExpectColumnNestedToExist(Expectation):
    """
    Expect a nested column to exist.
    """

    def _validate(self, configuration: ExpectationConfiguration, metrics: Dict, runtime_configuration: dict = None, execution_engine: ExecutionEngine = None):
        column = configuration.kwargs.get("column")
        nested_column = configuration.kwargs.get("nested_column")
        column_values = metrics.get("column_values.nonnull")
        result = all(nested_column in row[column] for row in column_values)
        return {"success": result}

    @classmethod
    def _get_evaluation_dependencies(cls, configuration: ExpectationConfiguration, execution_engine: ExecutionEngine, runtime_configuration: dict = None):
        return {
            "column_values.nonnull": MetricConfiguration(
                "column_values.nonnull",
                metric_domain_kwargs=configuration.kwargs,
                metric_value_kwargs={"ignore_row_if": "all_values_are_missing"},
            )
        }

    map_metric = "column_values.nonnull"
    success_keys = ("column", "nested_column")

    @classmethod
    def _prescriptive_renderer(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs
    ):
        runtime_configuration = runtime_configuration or {}
        include_column_name = runtime_configuration.get("include_column_name", True)
        include_column_name_in_message = runtime_configuration.get(
            "include_column_name_in_message", include_column_name
        )
        column = configuration.kwargs.get("column")
        nested_column = configuration.kwargs.get("nested_column")
        template_str = f"values must have a key named {nested_column} in the {column} object."
        params = {}
        if include_column_name_in_message:
            template_str = "$column " + template_str
            params["column"] = column
        return [RenderedStringTemplateContent(**{"content_block_type": "string_template", "string_template": {"template": template_str, "params": params, "styling": {"default": {"classes": ["badge", "badge-secondary"]}, "params": {"column": {"classes": ["badge", "badge-primary"]}}}}})]

if __name__ == "__main__":
    ExpectColumnNestedToExist().print_diagnostic_checklist()