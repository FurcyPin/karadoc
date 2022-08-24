import unittest

from karadoc.common.validations import ValidationSeverity


class TestValidationPackage(unittest.TestCase):
    def test_validation_severity_decrease(self):
        self.assertEqual(ValidationSeverity.Error, ValidationSeverity.Critical.decrease())
        self.assertEqual(ValidationSeverity.Debug, ValidationSeverity.Debug.decrease())

    def test_validation_severity_increase(self):
        self.assertEqual(ValidationSeverity.Critical, ValidationSeverity.Critical.increase())
        self.assertEqual(ValidationSeverity.Info, ValidationSeverity.Debug.increase())
