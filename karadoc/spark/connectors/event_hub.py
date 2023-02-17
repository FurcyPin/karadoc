import json

from pyspark.sql import SparkSession

from karadoc.common.conf import ConfBox
from karadoc.common.conf.configurable_class import ConfParam
from karadoc.common.utils.assert_utils import assert_true
from karadoc.spark.spark_connector import ConfigurableSparkConnector


class EventHubConnector(ConfigurableSparkConnector):
    HubName = ConfParam(required=True, type=str, description="Name of the Event Hub.")
    SharedAccessKeyName = ConfParam(
        required=True, type=str, description="Name of the shared access policy (SAS Policy)"
    )
    SharedAccessKey = ConfParam(
        required=True,
        type=str,
        secret=True,
        description="Key of the shared access policy",
    )

    def __init__(self, spark: SparkSession, connection_conf: ConfBox):
        ConfigurableSparkConnector.__init__(self, spark, connection_conf)

    def _configure(
        self,
        entity_path,
        starting_time,
        ending_time,
        consumer_group,
        offset=None,
        max_events_per_trigger=None,
        options=None,
    ):
        if options is None:
            options = {}

        hub_name = self.HubName.get()
        shared_access_key_name = self.SharedAccessKeyName.get()
        shared_access_key = self.SharedAccessKey.get()

        connection_string = (
            f"Endpoint=amqps://{hub_name}.servicebus.windows.net/{hub_name};EntityPath={entity_path};"
            f"SharedAccessKeyName={shared_access_key_name};SharedAccessKey={shared_access_key}"
        )

        starting_event_position = {
            "offset": offset,  # not in use
            "seqNo": -1,  # not in use
            "enqueuedTime": starting_time,
            "isInclusive": True,
        }

        ending_event_position = {
            "offset": None,  # not in use
            "seqNo": -1,  # not in use
            "enqueuedTime": ending_time,
            "isInclusive": False,
        }
        max_events_per_trigger_conf = {}
        if max_events_per_trigger is not None:
            max_events_per_trigger_conf = {"maxEventsPerTrigger": max_events_per_trigger}
        eh_conf = {
            "eventhubs.connectionString": self.spark._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(
                connection_string
            ),
            "eventhubs.startingPosition": json.dumps(starting_event_position),
            "eventhubs.endingPosition": json.dumps(ending_event_position),
            "eventhubs.consumerGroup": consumer_group,
            **max_events_per_trigger_conf,
            **options,
        }

        return eh_conf

    @staticmethod
    def _bad_time_format_message(time_var: str) -> str:
        return f"Bad time format for eventhub, should end with 000000Z but got: {time_var}"

    def read(self, source: dict):
        """Read data from an external input via an Event Hub connection.

        The source must have the following attributes:

        - "connection": name of the event_hub connection
        - "entity_path": name of the topic to follow
        - "starting_time": must use the format '%Y-%m-%dT%H:%M:%S.%fZ'
        - "ending_time": must use the format '%Y-%m-%dT%H:%M:%S.%fZ'
        - "consumer_group" (Optional): event_hub consumer group name
        - "options" (Optional): additional options can be added, here is the list of event-hubs-configuration options :
            https://github.com/Azure/azure-event-hubs-spark/blob/master/docs/structured-streaming-eventhubs-integration.md#eventhubsconf

        Because of this bug https://github.com/Azure/azure-event-hubs-spark/issues/444
        we make an assert to prevent retrieving events outside the time bound

        EventHub consumer group documentation:
        https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-features#event-consumers
        Spark EventHub Connector Documentation:
        https://github.com/Azure/azure-event-hubs-spark/tree/master/docs

        The corresponding event_hub connection must have the following attributes:

        - type = 'event_hub'
        - HubName = name of the hub
        - SharedAccessKeyName = name of the shared access policy (SAS Policy)
        - SharedAccessKey = key of the shared access policy

        :param source: a dictionary describing the source
        :return: a DataFrame (conversationId, destinationAddress, host, message, messageId, messageType, sourceAddress)
        """
        assert_true(
            source["starting_time"][-7:] == "000000Z", self._bad_time_format_message(source["starting_time"][-7:])
        )
        assert_true(source["ending_time"][-7:] == "000000Z", self._bad_time_format_message(source["ending_time"][-7:]))
        conf = self._configure(
            entity_path=source["entity_path"],
            starting_time=source["starting_time"],
            ending_time=source["ending_time"],
            consumer_group=source.get("consumer_group", "$Default"),
            options=source.get("options"),
        )
        df = self.spark.read.format("eventhubs").options(**conf).load()
        return df

    def read_stream(self, source: dict):
        """Read data from an external input via an Event Hub connection in streaming.

        The source must have the following attributes:

        - "connection": name of the event_hub connection
        - "entity_path": name of the topic to follow
        - "starting_time" or "offset :

            - "starting_time" must use the format '%Y-%m-%dT%H:%M:%S.%fZ'
            - "offset" can be a number wrapped in a string (e.g.:'21235') or '@latest' or '@earliest'
        - "consumer_group" (Optional): event_hub consumer group name
        - "maxEventsPerTrigger" (Optional): Rate limit on maximum number of events processed per trigger interval
        - "options" (Optional): additional options can be added, here is the list of event-hubs-configuration options :
            https://github.com/Azure/azure-event-hubs-spark/blob/master/docs/PySpark/structured-streaming-pyspark.md#event-hubs-configuration

        Because of this bug https://github.com/Azure/azure-event-hubs-spark/issues/444
        we make an assert to prevent retrieving events outside the time bound

        EventHub consumer group documentation:
        https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-features#event-consumers
        Spark EventHub Connector Documentation:
        https://github.com/Azure/azure-event-hubs-spark/tree/master/docs

        The corresponding event_hub connection must have the following attributes:

        - type = 'event_hub'
        - HubName = name of the hub
        - SharedAccessKeyName = name of the shared access policy (SAS Policy)
        - SharedAccessKey = key of the shared access policy

        :param source: a dictionary describing the source
        :return: a DataFrame (conversationId, destinationAddress, host, message, messageId, messageType, sourceAddress)
        """
        assert_true(source.get("starting_time") or source.get("offset"))
        if "starting_time" in source:
            assert_true(
                source["starting_time"][-7:] == "000000Z", self._bad_time_format_message(source["starting_time"][-7:])
            )
        if "ending_time" in source:
            assert_true(
                source["ending_time"][-7:] == "000000Z", self._bad_time_format_message(source["ending_time"][-7:])
            )
        conf = self._configure(
            entity_path=source["entity_path"],
            starting_time=source.get("starting_time"),
            ending_time=source.get("ending_time"),
            consumer_group=source.get("consumer_group", "$Default"),
            offset=source.get("offset"),
            max_events_per_trigger=source.get("maxEventsPerTrigger"),
            options=source.get("options"),
        )
        df = self.spark.readStream.format("eventhubs").options(**conf).load()
        return df
