from unittest import TestCase

from karadoc.spark.quality.exec import load_runnable_quality_check
from karadoc.test_utils.mock_settings import mock_settings_for_test_class
from tests.karadoc.test_utils import get_resource_folder_path


@mock_settings_for_test_class(
    {
        "enable_file_index_cache": False,
        "model_dir": get_resource_folder_path(__name__) + "/models",
    }
)
class TestExec(TestCase):
    def test_load_quality_check(self):
        job = load_runnable_quality_check("test_schema.test_table", {})
        alert_names = {a.name for a in job.alerts}
        expected_alert_names = {"test_alert_ok", "test_alert_ko"}
        self.assertEqual(expected_alert_names, alert_names)

        metric_names = {a.name for a in job.metrics}
        expected_metric_names = {"test_metric"}
        self.assertEqual(expected_metric_names, metric_names)

    def test_checks_must_be_sorted_by_definition_order(self):
        job = load_runnable_quality_check("test_schema.checks_must_be_sorted_by_definition_order", {})
        alert_names = [a.name for a in job.alerts]
        expected_alert_names = ["alert_B", "alert_A", "alert_C"]
        self.assertEqual(expected_alert_names, alert_names)

        metric_names = [a.name for a in job.metrics]
        expected_metric_names = ["metric_B", "metric_A", "metric_C"]
        self.assertEqual(expected_metric_names, metric_names)

    def test_checks_defined_with_a_template_must_be_sorted_by_definition_order(self):
        """
        Given a QUALITY_CHECK.py file where we define alerts and metrics using the "gen_alert" design principle
        When we load it
        Then the alerts and metrics should be defined and the alerts should be listed in their order of generation
        """
        job = load_runnable_quality_check(
            "test_schema.checks_defined_with_a_template_must_be_sorted_by_definition_order", {}
        )
        alert_names = [a.name for a in job.alerts]
        expected_alert_names = ["alert_1", "alert_2", "alert_3"]
        self.assertEqual(expected_alert_names, alert_names)

        metric_names = [a.name for a in job.metrics]
        expected_metric_names = ["metric_1", "metric_2", "metric_3"]
        self.assertEqual(expected_metric_names, metric_names)
