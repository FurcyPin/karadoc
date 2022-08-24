from logging import Handler, LogRecord


class DummyLogHandler(Handler):
    """Handles log export to Azure App Insight. Enriches the log records to be exported."""

    def __init__(self, **options):
        super(DummyLogHandler, self).__init__()

    def emit(self, record: LogRecord) -> None:
        """Abstract method to implement in custom handlers
        :param record: log record to export to azure
        """
        print("DummyLogHandler: %s" % record.msg)
