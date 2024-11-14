from unittest.mock import Mock

def mock_get_responses():
    # URL: /MetadataInstances/tablesValue
    # 用途: 模拟获取表ID的API响应
    return [
        Mock(status_code=200, json=lambda: {
            "code": "ok",
            "data": [
                {"tableId": "672b2d7c9eb272098572355f", "tableName": "ReplicationTableEditor"}
            ]
        }),
        Mock(status_code=200, json=lambda: {
            "code": "ok",
            "data": {
                "fields": [
                    {"name": "_id", "primaryKey": True},
                    {"name": "SETTLED_DATE", "primaryKey": False}
                ]
            }
        })
    ]

def mock_get_responses_for_operations():
    # URL: /MetadataInstances/tablesValue
    # 用途: 模拟获取表字段信息的API响应，用于操作测试
    return [
        Mock(status_code=200, json=lambda: {
            "code": "ok",
            "data": [
                {"tableId": "672b2d7c9eb272098572355f", "tableName": "ReplicationTableEditor"}
            ]
        }),
        Mock(status_code=200, json=lambda: {
            "code": "ok",
            "data": {
                "fields": [
                    {"name": "field1", "primaryKey": False},
                    {"name": "field2", "primaryKey": True}
                ]
            }
        })
    ]

def mock_req_get_for_metadata_instances():
    # URL: /MetadataInstances
    # 用途: 模拟获取元数据实例的API响应，返回表ID和字段信息
    return Mock(status_code=200, json=lambda: {
        "data": {
            "items": [
                {
                    "id": "table_id_123",
                    "original_name": "test_table",
                    "fields": [
                        {"field_name": "id", "primaryKey": True},
                        {"field_name": "name", "primaryKey": False}
                    ]
                }
            ]
        }
    })

def mock_req_get_for_metadata_instance_details():
    # URL: /MetadataInstances/{table_id}
    # 用途: 模拟获取特定元数据实例详情的API响应，返回字段信息
    return Mock(status_code=200, json=lambda: {
        "data": {
            "fields": [
                {"field_name": "id", "primaryKey": True},
                {"field_name": "name", "primaryKey": False}
            ]
        }
    })

def mock_req_post_for_metadata_v3(database_type, table_name, fields):
    # URL: /MetadataInstances/metadata/v3
    # 用途: 模拟获取元数据v3的API响应，返回表的元数据信息
    return Mock(status_code=200, json=lambda: {
        "reqId": "b2ec30d2-c118-43ed-8202-ec6e06dbbc99",
        "ts": 1731569721616,
        "code": "ok",
        "data": {
            "672065f6d6c2485881e9dd08": [
                {
                    "qualifiedName": f"T_{database_type}_io_tapdata_1_0-SNAPSHOT_{table_name}_672065f6d6c2485881e9dd08",
                    "tapTable": {
                        "lastUpdate": 1731569660772,
                        "nameFieldMap": fields,
                        "indexList": [
                            {
                                "name": "__t__{\"v\": 2, \"key\": {\"_id\": 1}, \"name\": \"_id_\"}",
                                "indexFields": [{}],
                                "unique": False,
                                "primary": False
                            }
                        ],
                        "id": table_name,
                        "name": table_name,
                        "tableAttr": {
                            "size": 224,
                            "ns": f"autoTest.{table_name}",
                            "capped": False,
                            "storageSize": 4096,
                            "avgObjSize": 224,
                            "shard": {}
                        },
                        "partitionIndex": {
                            "indexFields": [
                                {
                                    "name": "_id",
                                    "fieldAsc": True
                                }
                            ],
                            "unique": True,
                            "indexMap": {
                                "_id": {
                                    "name": "_id",
                                    "fieldAsc": True
                                }
                            }
                        },
                        "ancestorsName": table_name,
                        "maxPos": len(fields),
                        "maxPKPos": 1
                    }
                }
            ]
        }
    }) 