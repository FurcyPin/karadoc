from karadoc.common.commands.return_code import ReturnCode
from karadoc.common.conf.configurable_class import (
    ValidationResult_MissingRequiredParam,
    ValidationResult_SecretParam,
    ValidationResult_UnknownParam,
    ValidationResult_WrongParamType,
)
from karadoc.common.validations.connection_validations import (
    ValidationResult_ConnectionLoadError,
    ValidationResult_DisabledConnection,
    ValidationResult_MissingConnection,
    ValidationResult_MissingConnectionConfiguration,
    ValidationResult_MissingConnectionConfigurationDisabledJob,
)
from tests.karadoc.common.validations.validation_test_template import ValidationTestTemplate
from tests.karadoc.test_utils import get_resource_folder_path

test_resource_dir = get_resource_folder_path(__name__)


class TestConnectionValidations(ValidationTestTemplate):
    def test_validate_missing_connection(self):
        """
        - Given an action file using a missing connection
        - When we run `validate` on it
        - Then a ValidationResult_MissingConnection is yielded
        - And the command fails
        """
        self.validation_test_template(
            test_model_dir=f"{test_resource_dir}/models_missing_connection",
            expected_template=ValidationResult_MissingConnection,
            expected_return_code=ReturnCode.Error,
        )

    def test_validate_missing_connection_conf(self):
        """
        - Given an action file using a connection referencing an unknown connector
        - When we run `validate` on it
        - Then a ValidationResult_MissingConnectionConfiguration is yielded
        - And the command fails
        """
        self.validation_test_template(
            test_model_dir=f"{test_resource_dir}/models_missing_connection_conf",
            expected_template=ValidationResult_MissingConnectionConfiguration,
            expected_return_code=ReturnCode.Error,
        )

    def test_validate_missing_connection_conf_disabled_job(self):
        """
        - Given a disabled action file using a connection referencing an unknown connector
        - When we run `validate` on it
        - Then a ValidationResult_MissingConnectionConfigurationDisabledJob is yielded
        - And the command does not fail (because it is only a warning)
        """
        self.validation_test_template(
            test_model_dir=f"{test_resource_dir}/models_missing_connection_conf_disabled_job",
            expected_template=ValidationResult_MissingConnectionConfigurationDisabledJob,
            expected_return_code=ReturnCode.Success,
        )

    def test_validate_connection_loading_error(self):
        """
        - Given a disabled action file using a connection referencing connector that fails to load
        - When we run `validate` on it
        - Then a ValidationResult_ConnectionLoadError is yielded
        - And the command fails
        """
        settings = {"connection": {"test_conn": {"type": "unkown_connector_type"}}}
        self.validation_test_template(
            test_model_dir=f"{test_resource_dir}/models_connection_loading_error",
            expected_template=ValidationResult_ConnectionLoadError,
            expected_return_code=ReturnCode.Error,
            settings=settings,
        )

    def test_validate_disabled_connection(self):
        """
        - Given an action file using a disabled connection
        - When we run `validate` on it
        - Then a ValidationResult_DisabledConnection is yielded
        - And the command fails
        """
        settings = {"connection": {"test_conn": {"disable": "true", "type": "tests.resources.spark.connectors.dummy"}}}
        self.validation_test_template(
            test_model_dir=f"{test_resource_dir}/models_disabled_connection",
            expected_template=ValidationResult_DisabledConnection,
            expected_return_code=ReturnCode.Error,
            settings=settings,
        )

    def test_validate_configurable_connector(self):
        """
        - Given an action file using a ConfigurableConnector
        - When we run `validate` on it
        - Then no ValidationResult is yielded
        - And the command succeeds
        """
        settings = {
            "connection": {
                "test_conn": {
                    "type": "tests.resources.spark.connectors.dummy_configurable",
                    "a": "value",
                    "s.secret.vault": "secret_name",
                }
            }
        }
        self.validation_test_template(
            test_model_dir=f"{test_resource_dir}/models_ok",
            expected_template=None,
            expected_return_code=ReturnCode.Success,
            settings=settings,
        )

    def test_validate_configurable_connector_missing_required_param(self):
        """
        - Given an action file using a ConfigurableConnector with a missing required parameter
        - When we run `validate` on it
        - Then a ValidationResult_MissingRequiredParam is yielded
        - And the command fails
        """
        settings = {
            "connection": {
                "test_conn": {
                    "type": "tests.resources.spark.connectors.dummy_configurable",
                    "s.secret.vault": "secret_name",
                }
            }
        }
        self.validation_test_template(
            test_model_dir=f"{test_resource_dir}/models_ok",
            expected_template=ValidationResult_MissingRequiredParam,
            expected_return_code=ReturnCode.Error,
            settings=settings,
        )

    def test_validate_configurable_connector_wrong_type(self):
        """
        - Given an action file using a ConfigurableConnector with a parameter of the wrong type
        - When we run `validate` on it
        - Then a ValidationResult_WrongParamType is yielded
        - And the command fails
        """
        settings = {
            "connection": {
                "test_conn": {
                    "type": "tests.resources.spark.connectors.dummy_configurable",
                    "a": 1,
                    "s.secret.vault": "secret_name",
                }
            }
        }
        self.validation_test_template(
            test_model_dir=f"{test_resource_dir}/models_ok",
            expected_template=ValidationResult_WrongParamType,
            expected_return_code=ReturnCode.Error,
            settings=settings,
        )

    def test_validate_configurable_connector_unknown_param(self):
        """
        - Given an action file using a ConfigurableConnector with an unknown parameter
        - When we run `validate` on it
        - Then a ValidationResult_UnknownParam is yielded
        - And the command fails
        """
        settings = {
            "connection": {
                "test_conn": {
                    "type": "tests.resources.spark.connectors.dummy_configurable",
                    "a": "value",
                    "b": "unknown",
                    "s.secret.vault": "secret_name",
                }
            }
        }
        self.validation_test_template(
            test_model_dir=f"{test_resource_dir}/models_ok",
            expected_template=ValidationResult_UnknownParam,
            expected_return_code=ReturnCode.Error,
            settings=settings,
        )

    def test_validate_configurable_connector_secret_param(self):
        """
        - Given an action file using a ConfigurableConnector with a parameter which should be secret but isn't
        - When we run `validate` on it
        - Then a ValidationResult_SecretParam is yielded
        - But the command should not fail (this is only a warning)
        """
        settings = {
            "connection": {
                "test_conn": {
                    "type": "tests.resources.spark.connectors.dummy_configurable",
                    "a": "value",
                    "s": "should_be_secret",
                }
            }
        }
        self.validation_test_template(
            test_model_dir=f"{test_resource_dir}/models_ok",
            expected_template=ValidationResult_SecretParam,
            expected_return_code=ReturnCode.Success,
            settings=settings,
        )
