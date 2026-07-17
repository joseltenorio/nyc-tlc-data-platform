from tlc_data_platform.mongodb.index_manager import MongoIndexManager


class FakeCollection:
    def __init__(self):
        self.indexes = []

    def create_index(self, keys, **kwargs):
        self.indexes.append((keys, kwargs))


class FakeDatabase(dict):
    def __missing__(self, key):
        value = FakeCollection()
        self[key] = value
        return value


def test_creates_required_mongodb_indexes(app_config):
    database = FakeDatabase()
    MongoIndexManager(database, app_config.mongo).ensure_indexes()
    names = app_config.mongo.collections
    assert database[names.file_registry].indexes[0][1]["unique"] is True
    assert database[names.file_versions].indexes[0][1]["unique"] is True
    assert len(database[names.pipeline_executions].indexes) == 2
