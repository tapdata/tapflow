## Overview

TapFlow is an API framework for TapData Live Data Platform. It provides a programmable interace to interact with TapData, such as managing replication pipelines, building wide tables/materialized views and perform general data integration tasks. 

Currently a Python SDK and interative CLI shell(Tap Shell) is available in Preview mode. 

## Use cases
You may use TapFlow for:

- Speed up the query performance of your relational database by serving the data in a high performance data store
- Create a materialized view for publishing API
- Create a wide table in data warehouse for fast analytics
- As a Kafka ETL alternative to build real time ata pipelines
- As Oracle Golden Date alternatie to sync data between different databases

## Quick Start

🔔 **Prerequisite:** You have provisioned a TapData cluster. The easiest way is sign up a free cloud account from [TapData Cloud](https://cloud.tapdata.io).


1. Install the CLI utility Tap Shell, which comes with Python SDK/API

 ```
pip3 install tapcli
 ```

2. Follow the on-screen instruction to configure the TapData Cluster

3. Once you have installed and configured, follow these links to create your data pipelines

- [Quick start tutorial](https://deploy-preview-127--tapdata-en.netlify.app/tapflow/quick-start)
- [Build a continously updated materized view](https://deploy-preview-127--tapdata-en.netlify.app/tapflow/tapflow-tutorial/build-real-time-wide-table)
- [Tap Shell Command Reference](https://deploy-preview-127--tapdata-en.netlify.app/tapflow/tapcli-reference)


## Get in touch

- [Send Email](mailto:team@tapdata.io)
- [Slack channel](https://join.slack.com/t/tapdatacommunity/shared_invite/zt-1biraoxpf-NRTsap0YLlAp99PHIVC9eA)


## Roadmap

- Project support: where you can define a set of data flows and sequences of running
- API publishing capability
- Integration with 3rd party scheduler
- Java API/SDK


 
