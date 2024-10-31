
class Summarizer:
    """The Blob-Storage implementation."""
    _account_url: str
    _queue_name: str
    _max_messages: int

    def __init__(
        self,
        account_url: str | None,
        queue_name: str,
        client_id: str,
        max_message: int = 1
    ):
        self._account_url = account_url
        self._queue_name = queue_name
        self._max_messages = max_message
