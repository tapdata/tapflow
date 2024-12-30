[![PyPI Downloads](https://static.pepy.tech/badge/tapflow)](https://pepy.tech/projects/tapflow)

## What is TapFlow

TapFlow is a newly launched programming API framework for the TapData Live Data Platform. It allows developers and data engineers to build data pipelines and models using a simple powerful programming language.

This release includes a Python SDK. TapFlow requires a connection to a TapData Cluster, which can be either the enterprise, cloud, or community version, to operate.

![Tapdata Live Data Platform](docs/images/tapdata_live_data_platform.png)

## Why is a Programmatic Approach Needed ?

TapData currently provides a visual drag-and-drop UI for building data pipelines and workflows. This interface offers significant advantages in terms of ease of use and maintenance, but it also has limitations in certain areas. TapFlow aims to effectively address these limitations by offering a programmatic approach as a valuable complement.

1. Supporting Developers with Custom Logic: When Complex Processing Needs Code

For complex tasks like data masking or custom field value standardization, a significant amount of code is often required. When frequent changes are needed, relying on UI-based operations becomes less convenient.

2. Better Support for CI/CD and Automation

Teams often need to deploy and update data integration tasks across multiple environments (such as development, testing, and production). With a programmatic API, tasks can be automatically generated and migrated through scripts, making it easy to integrate with version control tools like Git, without the need for manual operations in the GUI.

3. Reducing Operational Costs in Complex Scenarios

Users often need to synchronize 100+ database tables, each with different field mapping rules. Manually setting these rules in the GUI is not only time-consuming but also prone to errors. With a programmatic API, these rules can be automatically generated, streamlining the process and reducing the chance of mistakes.

4. Developer-Friendly and Easier Workflow Integration

Providing tools designed for developers aligns more closely with their workflows, making it easier to integrate with other business modules. Open source code capabilities also make the product more extensible, allowing for the addition of reusable internal components and other custom features.

## TapFlow Usage Example

Let's assume we have a CRM application running on a MySQL database. Below is the schema for this MySQL database:

![Mysql Schema](docs/images/mysql_schema.png)

Due to performance considerations and specific requirements for wide tables, we need to replicate order data from MySQL to MongoDB in order to publish an order query API. We will use TapFlow to copy the data from MySQL to MongoDB, while performing some data transformations and merging operations along the way.

### Installing TapFlow

```
# pip3 install tapflow
```

The TapFlow Python SDK supports two modes: programmatic execution or interactive mode. In the following example, we'll demonstrate how to use the TapFlow API in interactive mode.

```
# tap

--- Please configure TapData cluster ---

Tap Flow requires TapData Live Data Platform(LDP) cluster to run. 
If you would like to use with TapData Enterprise or TapData Community, type L to continue. 
If you would like to use TapData Cloud, or you are new to TapData, type C or press ENTER to continue.
Please type L or C (L/[C]):
```

Once you enter the relevant TapData connection information or keys, you can start using TapFlow in interactive mode.

For more installation details, please refer to the Quick Start documentation: https://docs.tapdata.net/tapflow/quick-start

### Create Source and Target Database Connections

```
tap> mysql_conn = DataSource('mysql', 'MySQL_ECommerce', 
                    {
                    'database': 'ECommerceData',  
                    'port': 3306,                 
                    'host': 'demo.tapdata.io',      
                    'username': 'demouser',  
                    'password': 'demopass'   
                    })
                    .type('source')
                    .save()                  

tap> mongodb_conn = DataSource("mongodb", "MongoDB_ECommerce",
                    {
                        "uri": "mongodb://your_username:your_passwd@192.168.1.18:27017/ecommerce?authSource=admin"
                    })
                    .type("target")
                    .save()
```

### Create a Simple Order Table Replication Task (Data Flow)

**Objective**: Sync order data from MySQL to MongoDB for unified order queries.

```
tap> myflow = Flow("simple_data_replication_flow") \
       .read_from("MySQL_ECommerce.ecom_orders") \
       .write_to("MongoDB_ECommerce.orders_replica_collection") \
       .save()
tap> myFlow.start()
```

### Exclude Fields

**Objective**: The target table does not require all fields from the source table.

```
tap> myflow = Flow("order_fields_exclude_flow") \
       .read_from("MySQL_ECommerce.ecom_orders") \
       .exclude("order_delivered_carrier_date", "order_delivered_customer_date")  \
       .write_to("MongoDB_ECommerce.orders_renamed_fields_collection") \
       .save()
tap> myFlow.start()
```

### Field Rename

```
tap> myflow = Flow("order_fields_rename_flow") \
       .read_from("MySQL_ECommerce.ecom_orders") \
       .rename_fields({'order_purchase_timestamp': 'purchase_timestamp'})  \
       .write_to("MongoDB_ECommerce.orders_exclude_fields_collection") \
       .save()
tap> myFlow.start()
```

### Perform Complex Custom Processing with Python Script

```
# Standardize the order_status field value
tap> def pyfunc(record):
    if not record['order_status'] :
        record['order_status']  = 'invalid'
    if record['order_status'] == 'SendError' :
        record['order_status'] = 'undeliverable'
    return record # Returns the processed records

# Create a data flow task, apply the Python function, and write the data to the target database
tap> myflow = Flow("orders_complex_data_processing_flow") \
       .read_from("MySQL_ECommerce.ecom_orders") \
       .py(pyfunc) \
       .write_to("MongoDB_ECommerce.orders_processed_collection") \
       .save()
tap> myFlow.start()
```

### Use Lookup to Enrich Customer Information in the Order Table

```
tap> myflow = Flow("orders_lookup_flow") \
       .read_from("MySQL_ECommerce.ecom_orders") \
       .lookup("MySQL_ECommerce.ecom_customers", relation=[["customer_id", "customer_id"]]) \
       .write_to("MongoDB_ECommerce.wide_orders_collection") \
       .save()
tap> myFlow.start()
```

### Final Result In Monogdb

```
tap> use MongoDB_ECommerce
datasource switch to: MongoDB_ECommerce

tap> peek wide_orders_collection
table wide_orders_collection has 12641 records
{
  'order_status': 'unavailable',
  'order_purchase_timestamp': '2017-10-21 19:32:06',
  'customer_state': 'SP',
  'customer_unique_id': 'a77550dd00887c5bb24100ccbd08cbe9',
  'order_estimated_delivery_date': '2017-11-03 00:00:00',
  '_id': '676391b15338b293cc8525f1',
  'customer_id': '3a92efdb6e6163dc1734d44f2f5f6d04',
  'order_id': '0010dedd556712d7bb69a19cb7bbd37a',
  'order_approved_at': '2017-10-24 03:25:32',
  'customer_city': 'sao paulo',
  'customer_zip_code_prefix': '04851'
}
{
  'order_status': 'unavailable',
  'order_purchase_timestamp': '2017-02-14 14:49:29',
  'customer_state': 'SP',
  'customer_unique_id': '6968d01009b952ed987de42a239d7fa0',
  'order_estimated_delivery_date': '2017-03-13 00:00:00',
  '_id': '676391b15338b293cc8525f3',
  'customer_id': '1541ebabf956d17f3afe883790bd7dd3',
  'order_id': '00bca4adac549020c1273714d04d0208',
  'order_approved_at': '2017-02-14 15:03:38',
  'customer_city': 'campinas',
  'customer_zip_code_prefix': '13090'
}
{
  'order_status': 'canceled',
  'order_purchase_timestamp': '2017-04-10 00:45:56',
  'customer_state': 'SC',
  'customer_unique_id': 'b9badb100ff8ecc16a403111209e3a06',
  'order_estimated_delivery_date': '2017-05-04 00:00:00',
  '_id': '676391b15338b293cc8525f4',
  'customer_id': 'e3626ed5d1f2e3a02d692ec948b25eeb',
  'order_id': '00ff0cf5583758e6964723e42f111bf4',
  'order_approved_at': '2017-04-10 01:03:29',
  'customer_city': 'florianopolis',
  'customer_zip_code_prefix': '88025'
}
{
  'order_status': 'canceled',
  'order_purchase_timestamp': '2018-02-08 21:09:18',
  'customer_state': 'SP',
  'customer_unique_id': '2fbe21ee78c83f908ed47ba9f4a74121',
  'order_estimated_delivery_date': '2018-02-22 00:00:00',
  '_id': '676391b15338b293cc8525f6',
  'customer_id': '19e9dac43eee8df98092482fdce676da',
  'order_id': '0166cc8756d58b4a30fb49de83527120',
  'order_delivered_carrier_date': '2018-02-09 23:12:30',
  'order_approved_at': '2018-02-08 21:27:56',
  'customer_city': 'jundiai',
  'customer_zip_code_prefix': '13203'
}
{
  'order_status': 'unavailable',
  'order_purchase_timestamp': '2017-01-31 17:48:07',
  'customer_state': 'MA',
  'customer_unique_id': '859801343e938e403027c0668f64f037',
  'order_estimated_delivery_date': '2017-03-10 00:00:00',
  '_id': '676391b15338b293cc8525f7',
  'customer_id': 'cf52d3f2132b17f52fdf4b40c135163e',
  'order_id': '01cb6d702e5233235f4125309d184bf4',
  'order_approved_at': '2017-02-01 02:45:37',
  'customer_city': 'sao luis',
  'customer_zip_code_prefix': '65054'
}
```

For more commands references, please refer to this documentation:  https://docs.tapdata.net/tapflow/tapcli-reference

### Run Python Code using TapFlow command

```
# cat order_mview.py
from tapflow.lib import *
from tapflow.cli.cli import init
                    
mysql_conn = DataSource('mysql', 'MySQL_ECommerce', 
                    {
                    'database': 'ECommerceData',  
                    'port': 3306,                 
                    'host': 'demo.tapdata.io',      
                    'username': 'demouser',  
                    'password': 'demopass'   
                    })
                    .type('source')
                    .save()   
                    
mongodb_conn = DataSource("mongodb", "MongoDB_ECommerce",
                    {
                        "uri": "mongodb://your_username:your_passwd@192.168.1.18:27017/ecommerce?authSource=admin"
                    })
                    .type("target")
                    .save()

def pyfunc(record):
    if not record['order_status'] :
        record['order_status']  = 'invalid'
    if record['order_status'] == 'SendError' :
        record['order_status'] = 'undeliverable'
    return record
         
myflow = Flow("mysql_order_flow") \
       .read_from("MySQL_ECommerce.ecom_orders") \
       .exclude("order_delivered_carrier_date", "order_delivered_customer_date")  \
       .rename_fields({'order_purchase_timestamp': 'purchase_timestamp'})  \
       .py(pyfunc)
       .lookup("MySQL_ECommerce.ecom_customers", relation=[["customer_id", "customer_id"]]) \
       .write_to("MongoDB_ECommerce.wide_orders_collection") \
       .save()

myflow.start() # Start the flow
       
# python order_mview.py
```

## Known Issue

- Currently, Lookup can only be used with MongoDB as the target.

## TapFlow Roadmap

We will continue to enhance TapFlow's capabilities. Here are some medium- to long-term features on the roadmap:

- Lookup support for more target databases beyond MongoDB
- Support for Project management
- Support for inner joins
- Java SDK and RESTful API availability

## About TapData Live Data Platform

If you're not yet familiar, TapData is a real-time data platform designed specifically for enterprise-level real-time data integration and data services. It has the following features:

1. **Framework Designed for Real-Time Data Pipelines**: Based on CDC (Change Data Capture) technology, data collection and processing latency can be sub-second.
2. **High Performance**: Each node can process hundreds of thousands of records per second.
3. **Built-in Rich CDC Connectors**: Quickly integrate with Oracle, DB2, Sybase, MySQL, PostgreSQL, MSSQL, and more.
4. **Rich Real-Time Data Processing Functions**: Including filtering, renaming, and adding/removing fields.
5. **Multi-Table Associations**: Build continuously updated materialized views and aggregate data.
6. **UDF Support**: Custom JavaScript or Python functions to handle complex logic.
7. **At Least Once and Exactly Once Consistency Guarantees**.
8. **Comprehensive Data Validation**: Full and incremental validation, hash validation, secondary checks, etc.
9. **Support for Domestic (Mainland China) Databases**: Dameng, Kingbase, GaussDB, OceanBase, GBase, VastBase.
10. **Kafka Support**: Act as a producer to push database events directly to Kafka or consume events from Kafka queues.
11. **Private and Fully Managed Deployment Options**: Can be deployed offline with the open-source version or use the fully managed service on TapData Cloud.

## Common Use Cases of TapData

1. **Real-Time Database Synchronization (Replacing Oracle GoldenGate)**

Traditional database replication tools are often expensive and complex. TapFlow offers a lightweight, easy-to-use alternative that efficiently syncs data across different databases, supporting everything from MySQL to PostgreSQL and even NoSQL databases.

2. **Real-Time Data Pipeline (Replacing Kafka)**

For scenarios requiring real-time data transmission, TapFlow is a powerful alternative to Kafka. It doesn't require the deployment of complex Kafka clusters, but instead provides an equally or even more efficient data pipeline construction in a lightweight manner.

3. **Creating Continuously Refreshed Materialized Views for Query Acceleration and Read-Write Separation**

When real-time query results are needed, materialized views offer an efficient solution. TapFlow can continuously refresh materialized views (sub-second latency), ensuring data freshness and supporting real-time analysis and decision-making.

4. **Real-Time Data Ingestion into Data Warehouses or Data Lakes**

The trend in modern data analytics is real-time processing. TapFlow can write data in real-time into data warehouses or data lakes (e.g., Apache Doris, ClickHouse, or cloud data warehouses like Ali Cloud ADB, SelectDB, BigQuery).

5. **General-Purpose Streaming ETL Data Processing**

TapFlow also supports complex ETL (Extract, Transform, Load) tasks. With the flexibility of Python and built-in processing capabilities, developers can easily handle complex data transformation needs.

## Join Our Community

We welcome you to join our community and interact with us. You can:

- Star us on [GitHub](https://github.com/tapdata/tapdata)
- Join our [discord](https://discord.gg/xj79hnEB) community or [whatsApp group](https://chat.whatsapp.com/LeY1lVE5GPkEc3TZeEZgDF)

## Stargazers over time
[![Stargazers over time](https://starchart.cc/tapdata/tapflow.svg?variant=adaptive)](https://starchart.cc/tapdata/tapflow)