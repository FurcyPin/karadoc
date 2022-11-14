from karadoc.common.commands.return_code import ReturnCode
from karadoc.common.validations.quality_check_validations import (
    ValidationResult_QualityCheckAlertDuplicateName,
    ValidationResult_QualityCheckMetricDuplicateName,
)
from tests.karadoc.common.validations.validation_test_template import (
    ValidationTestTemplate,
)
from tests.karadoc.test_utils import get_resource_folder_path

test_resource_dir = get_resource_folder_path(__name__)


class TestQualityCheckValidations(ValidationTestTemplate):
    def test_validate_quality_check_duplicate_alert(self):
        """
        - Given a QUALITY_CHECK.py file with duplicate actions
        - When we run a validate on it
        - Then a ValidationResult_QualityCheckAlertDuplicateName is yielded
        - And the command fails
        """
        self.validation_test_template(
            test_model_dir=f"{test_resource_dir}/model_quality_check_duplicate_alerts",
            expected_template=ValidationResult_QualityCheckAlertDuplicateName,
            expected_return_code=ReturnCode.Error,
        )

    def test_validate_quality_check_duplicate_metric(self):
        """
        - Given a QUALITY_CHECK.py file with duplicate metrics
        - When we run a validate on it
        - Then a ValidationResult_QualityCheckMetricDuplicateName is yielded
        - And the command fails
        """
        self.validation_test_template(
            test_model_dir=f"{test_resource_dir}/model_quality_check_duplicate_metrics",
            expected_template=ValidationResult_QualityCheckMetricDuplicateName,
            expected_return_code=ReturnCode.Error,
        )
