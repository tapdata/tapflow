from tapflow.lib.data_pipeline.base_obj import BaseObj


class RenameTable(BaseObj):
    def __init__(self, prefix, suffix, tables, config=None):
        super().__init__()
        if config is None:
            config = []
        self.prefix = prefix
        self.suffix = suffix
        self.tables = tables
        self.config = config

    def to_dict(self):
        new_tables = []
        for table in self.tables:
            c = False
            for i in self.config:
                if i[0] == table:
                    new_tables.append({
                        "originTableName": table,
                        "previousTableName": table,
                        "currentTableName": i[1]
                    })
                    c = True
                    break
            if c:
                continue
            new_tables.append({
                "originTableName": table,
                "previousTableName": table,
                "currentTableName": self.prefix + table + self.suffix
            })
        return {
            "catalog": "processor",
            "elementType": "Node",
            "id": self.id,
            "name": "TableRename",
            "prefix": self.prefix,
            "suffix": self.suffix,
            "type": "table_rename_processor",
            "tableNames": new_tables
        }
