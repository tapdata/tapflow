## Overview

TapFlow is an API framework for TapData Live Data Platform. It provides a programmable interace to interact with TapData, such as managing replication pipelines, building wide tables/materialized views and perform general data integration tasks. 

Currently a Python SDK and interative CLI shell(Tap Shell) is available in Preview mode. 

## Use cases

You may use TapFlow for:

- As a Kafka alternative to build real time data pipelines
- Create a materialized view that is continuously refreshed
- As Oracle Golden Date alternatie to replicate data between different databases
- Feeding data into real time data warehouse / data lake
- General purpose data ETL 

## Why TapFlow

- A framework designed for real time data pipelines
- Sub-second low latency experience
- Easy to develop Python language
- Javascript UDF for complex processing
- Built-in CDC connectors for popular databases


## Quick Start

First, you need to have a TapData cluster ready if not already. You may choose one of the following methods:

- Sign up a free account on [TapData Cloud](https://cloud.tapdata.io) - recommended option. 
- Following this guide to install [Community Edition](https://docs.tapdata.io/installation/install-tapdata-community).
- Contact us for an [Enterprise Edition](https://tapdata.mike-x.com/lV5o0?m=KwbD6vkbRUwcRNCo).


After installing or sign up, you can install Tap Shell, a CLI utility that comes with Python SDK/API

 ```
pip3 install tapcli
 ```

Type 'tap' to enter into Tap Shell:

```
# tap
```

Follow the on-screen instruction to configure the connection to the TapData Cluster. 

## Docs

Read [TapFlow Docs](https://docs.tapdata.io/tapflow/) to learn more about how TapFlow can be used. 

Here are some of the articles you may find useful:

- [Quick start tutorial](https://deploy-preview-127--tapdata-en.netlify.app/tapflow/quick-start)
- [Build a continously updated materized view](https://deploy-preview-127--tapdata-en.netlify.app/tapflow/tapflow-tutorial/build-real-time-wide-table)
- [Tap Shell Command Reference](https://deploy-preview-127--tapdata-en.netlify.app/tapflow/tapcli-reference)




## Get in touch

- [Send Email](mailto:team@tapdata.io)
- [Slack channel](https://join.slack.com/t/tapdatacommunity/shared_invite/zt-1biraoxpf-NRTsap0YLlAp99PHIVC9eA)


## Roadmap

- Project/workflow support: running a set of data pipelines in coordination
- Publish API 
- Aggregation support
- Java API/SDK


 
