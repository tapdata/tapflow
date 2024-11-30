## Overview

Tap Flow is an API framework for TapData Live Data Platform. It provides a programmable interace to interact with TapData, such as managing replication pipelines, building wide tables/materialized views and perform general data integration tasks. 

Currently A Python SDK and interative CLI shell is provided in Tap Flow framework. 

## Use cases
You may use Tap Flow for:

- Speed up the query performance of your relational database by serving the data in a high performance data store
- Create a materialized view for publishing API
- Create a wide table in data warehouse for fast analytic
- As a Kafka ETL alternative to build real time ata pipelines
- As Oracle Golden Date alternatie to replicate data between different databases

## Quick Start

🔔 **Reminder:** You need a TapData cluster ready in order to continue. The easiest way is sign up a free [TapData Cloud](https://cloud.tapdata.io) account. 


1. Install Tap Shell, which comes with Python SDK/API

 ```
pip3 install tapcli
 ```

2. Follow the on-screen instruction to configure the TapData Cluster

3. Once you have installed and configured, follow these links to create your data pipelines

- [Quick start tutorial: creating your mysql to mongodb replication flow](https://deploy-preview-127--tapdata-en.netlify.app/tapflow/quick-start)
- [Build a continously updated materized view](https://deploy-preview-127--tapdata-en.netlify.app/tapflow/tapflow-tutorial/build-real-time-wide-table)
- [Tap CLI Command Reference](https://deploy-preview-127--tapdata-en.netlify.app/tapflow/tapcli-reference)


## When to use Tap Flow 

- When you have many pipelines, managing them manually from UI becomes difficult
- When you want to integrate TapData capability with your application or workflow, programmatically
- When you have multiple environments(Dev, QA, UAT, Prod) and you would like to version control your data pipelines
- When you have complex processing logic that must use Javascript or Python
- You just like coding experience!

## Join the community



## Roadmap

- Project support: where you can define a set of data flows and sequences of running
- API publishing capability
- Integration with 3rd party scheduler
- Java API/SDK


 
