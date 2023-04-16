# Introduction

This code was first made using Databricks Community, however it's not worth to put the .dbc file in github since the point is to show the code, therefore I brought the code to vscode and the pull it to github. Also, the focus here is to show knowledge in structured streaming, thus the code will not be very complex as I'm limited to the tools used. In the next lines I'll resume the project as well as the tecnologies 

# Tools

## Databricks

In order to write my code I used databricks community, which is a free version of databricks. I choose this version since the paid version is too expensive, hence I was provided with a cluster with a cluster with only 2 cores which suits me well.

Databricks is known as a Modern Data Warehouse(MDW), which is a DW hosted on the cloud and it's purpose is to process large amounts of data in any form, and by comparison with Traditional Data Warehouse(TDW) are cheaper since use the pay as you go concept.

Databricks is also the precursor of the Delta Lake architecture, that focus on bring more realiable data by adding a layer upon the Data Lake, this layer can be represented by the Medallion Architecture, that consists in create types of tables: Bronze, Silver and Gold. The first type is a table that has data in its raw state, eg, the way the data came from the source. The silver is the data from the bronze with a data munging and the last one is the data ready to be consumed by the business part of a company. Below you can find an example of Delta Lake Architecture:

![medallion_architecture](https://corgisandcode.com/wp-content/uploads/2021/02/medallion.png)


The bronze, silver and gold concepts will be used here as folders inside the DBFS(Databricks File System), that is, basically a file system for databricks.

## Structured Streaming

Structured Streaming is a fault tolerant and scalable way to stream data using a Spark SQL Engine, it is possible to express the streaming computation as if were a batch computation, in fact Spark Structured Streaming uses micro-batch processing engine that process the data stream as a series of small jobs achieving end to end latencies by the order of 100 miliseconds.

Structured Streaming also has the **exactly-once** fault tolerance with means that for each message handed to the mechanism exactly one delivery is made to the recipient; the message can neither be lost nor duplicated. Likewise it's possible to deliver **at-least-once guarantees** which means that for each message handed to the mechanism potentially multiple attempts are made at delivering it, such that at least one succeeds. This last guaranteed mode can be delivered with a latency of 1 milisecond, however can bring duplicated data as make multiple attempts of delivering a message.


# Project

As said before the point here it's not to show a complex problem, instead to show knowledge in structured streaming. Also I do not own a highly sophisticated structure(as it happens in most companies) hence I cannot solve sophisticated problems. The project basically read data from 16 json files, 8 about banks and 8 about credit cards. The first phase is to read the data from this files and store them in the bronze layer. After that I read from the bronze layer and then apply data munging to clean the data, later I stored it in the silver layer. At last, I read from the silver layer join the two dataframes to create another one that will be stored in the gold layer(therefore able to be accessed by the business area of a company)

![project](https://github.com/LucasbFontes/streaming_spark/blob/main/Screen%20Shot%202023-04-16%20at%2019.12.32.png?raw=true
)


 





