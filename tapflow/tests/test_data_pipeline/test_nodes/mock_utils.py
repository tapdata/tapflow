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