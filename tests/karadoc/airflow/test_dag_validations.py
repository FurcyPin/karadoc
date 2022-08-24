from karadoc.airflow.dag_validations import (
    ValidationResult_DagNotScheduledNorTriggered,
    ValidationResult_DagScheduledAndTriggered,
    ValidationResult_DagTriggeredMoreThanOnce,
)
from karadoc.common.commands.return_code import ReturnCode
from tests.karadoc.airflow.validation_test_template import ValidationTestTemplate

test_resource_dir = "tests/resources/karadoc/airflow/test_dag_validations"


class TestDagValidations(ValidationTestTemplate):
    def test_validate_dag_with_deps(self):
        """
        - Given a dag which has local code dependencies and no issue
        - When we run a "validate_airflow" command on it
        - Then the command succeeds
        """
        self.validation_test_template(
            test_airflow_dir=f"{test_resource_dir}/dag_with_deps",
            expected_template=None,
            expected_return_code=ReturnCode.Success,
        )

    def test_validate_dag_triggered_more_than_once(self):
        """
        - Given a dag which is triggered more than once
        - When we run a "validate_airflow" command on it
        - Then a ValidationResult_DagTriggeredMoreThanOnce is yielded
        - And the command succeeds
        """
        self.validation_test_template(
            test_airflow_dir=f"{test_resource_dir}/dag_triggered_more_than_once",
            expected_template=ValidationResult_DagTriggeredMoreThanOnce,
            expected_return_code=ReturnCode.Success,
        )

    def test_validate_dag_not_scheduled_nor_triggered(self):
        """
        - Given a dag which is not scheduled nor triggered
        - When we run a "validate_airflow" command on it
        - Then a ValidationResult_DagNotScheduledNorTriggered is yielded
        - And the command succeeds
        """
        self.validation_test_template(
            test_airflow_dir=f"{test_resource_dir}/dag_not_scheduled_nor_triggered",
            expected_template=ValidationResult_DagNotScheduledNorTriggered,
            expected_return_code=ReturnCode.Success,
        )

    def test_validate_dag_scheduled_and_triggered(self):
        """
        - Given a dag which is scheduled and triggered
        - When we run a "validate_airflow" command on it
        - Then a ValidationResult_DagScheduledAndTriggered is yielded
        - And the command succeeds
        """
        self.validation_test_template(
            test_airflow_dir=f"{test_resource_dir}/dag_scheduled_and_triggered",
            expected_template=ValidationResult_DagScheduledAndTriggered,
            expected_return_code=ReturnCode.Success,
        )
