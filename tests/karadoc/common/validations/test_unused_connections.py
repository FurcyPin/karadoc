from karadoc.common.commands.return_code import ReturnCode
from karadoc.common.validations.unused_connection_validations import (
    ValidationResult_ConnectionCouldBeDisabled,
    ValidationResult_UnusedConnection,
)
from tests.karadoc.common.validations.validation_test_template import ValidationTestTemplate
from tests.karadoc.test_utils import get_resource_folder_path

test_resource_dir = get_resource_folder_path(__name__)


class TestUnusedConnections(ValidationTestTemplate):
    def test_validate_unused_connection(self):
        """
        - Given a connection used by no action file
        - When we run a validate on it
        - Then a ValidationResult_UnusedConnection is yielded
        - And the command does not fail (because it is only a warning)
        """
        settings = {"connection": {"test_conn": {"type": "tests.resources.connectors.dummy"}}}
        self.validation_test_template(
            test_model_dir=f"{test_resource_dir}/model",
            expected_template=ValidationResult_UnusedConnection,
            expected_return_code=ReturnCode.Success,
            settings=settings,
        )

    def test_validate_connection_could_be_disabled(self):
        """
        - Given a connection used by no action file
        - When we run a validate on it
        - Then a ValidationResult_UnusedConnection is yielded
        - And the command does not fail (because it is only a warning)
        """
        settings = {"connection": {"test_conn": {"type": "tests.resources.connectors.dummy"}}}
        self.validation_test_template(
            test_model_dir=f"{test_resource_dir}/model_connection_could_be_disabled",
            expected_template=ValidationResult_ConnectionCouldBeDisabled,
            expected_return_code=ReturnCode.Success,
            settings=settings,
        )
