from framework.utils.singleton import SingletonMeta


class InProcessBatchManager(metaclass=SingletonMeta):
    def __init__(self):
        print("init InProcessBatch")
        self._batch_id = None
        self._elt_client = None
        self._is_simulation = None
        self._metadata = {}

    @property
    def batch_id(self):
        return self._batch_id

    @batch_id.setter
    def batch_id(self, value):
        self._batch_id = value

    @property
    def elt_client(self):
        return self._elt_client

    @elt_client.setter
    def elt_client(self, value: str):
        self._elt_client = value

    @property
    def is_simulation(self):
        return self._is_simulation

    @is_simulation.setter
    def is_simulation(self, value):
        self._is_simulation = value

    @property
    def metadata(self):
        return self._metadata

    def set_metadata(self, key, value):
        self._metadata[key] = value

    def clean_up(self):
        self._batch_id = None
        self._elt_client = None
        self._is_simulation = None
