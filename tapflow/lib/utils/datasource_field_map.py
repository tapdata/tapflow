# 后端提供的字段名存在不合适展示的情况，在这里做一个映射
datasource_field_map = {
    "addtionalString": "additionalString",
    "masterSlaveAddress": "primarySecondaryAddress",
}


# 反向映射
reverse_datasource_field_map = {
    "additionalString": "addtionalString",
    "primarySecondaryAddress": "masterSlaveAddress",
}