from karadoc.common.commands.return_code import ReturnCode
from karadoc.common.validations.job_validations import (
    ValidationResult_InvalidOutputPartition,
    ValidationResult_JobLoadError,
    ValidationResult_SecondaryKeyWithoutPrimaryKey,
    ValidationResult_UnknownInput,
)
from tests.karadoc.common.validations.validation_test_template import (
    ValidationTestTemplate,
)

test_resource_dir = "tests/resources/karadoc/common/validations/test_job_validations"


class TestJobValidations(ValidationTestTemplate):
    def test_validate_wrong_job_type(self):
        """
        - Given a action file with a Job of the wrong type defined in it
        - When we run a validate on it
        - Then a ValidationResult_JobLoadError is yielded
        - And the command fails
        """
        self.validation_test_template(
            test_model_dir=f"{test_resource_dir}/model_wrong_job_type",
            expected_template=ValidationResult_JobLoadError,
            expected_return_code=ReturnCode.Error,
        )

    def test_validate_unknown_input(self):
        """
        - Given a POPULATE.py file with an unknown input
        - When we run a validate on it
        - Then a ValidationResult_UnknownInput is yielded
        - And the command fails
        """
        self.validation_test_template(
            test_model_dir=f"{test_resource_dir}/model_unknown_input",
            expected_template=ValidationResult_UnknownInput,
            expected_return_code=ReturnCode.Error,
        )

    def test_validate_invalid_output_partition(self):
        """
        - Given a POPULATE.py file with an invalid output partition
        - When we run a validate on it
        - Then a ValidationResult_InvalidOutputPartition is yielded
        - And the command fails
        """
        self.validation_test_template(
            test_model_dir=f"{test_resource_dir}/model_invalid_output_partition",
            expected_template=ValidationResult_InvalidOutputPartition,
            expected_return_code=ReturnCode.Error,
        )

    def test_validate_secondary_key_without_primary_key(self):
        """
        - Given a POPULATE.py file with a secondary key without primary key
        - When we run a validate on it
        - Then a ValidationResult_SecondaryKeyWithoutPrimaryKey is yielded
        - And the command fails
        """
        self.validation_test_template(
            test_model_dir=f"{test_resource_dir}/model_secondary_key_without_primary_key",
            expected_template=ValidationResult_SecondaryKeyWithoutPrimaryKey,
            expected_return_code=ReturnCode.Error,
        )
