from auto_test.tapdata.data_pipeline.base_obj import BaseObj


class Agg(BaseObj):
    def __init__(self, name, method, key, groupKeys, pk=[], ttl=3600):
        super().__init__()
        self.name = name
        self.method = method
        self.key = key
        self.groupKeys = groupKeys
        self.pk = pk
        self.ttl = ttl

