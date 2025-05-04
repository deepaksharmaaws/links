Slide 1

Welcome everyone! Today we'll explore how to build a serverless medallion architecture using AWS services. I'm Deepak Sharma, Senior Delivery Consultant at AWS. This session will be particularly valuable for data engineers and architects looking to implement modern data lakes. We'll cover both theoretical concepts and practical implementation details followed by a quick demo 


Slide 2
Today, I'm excited to walk you through how we're implementing a modern data lakehouse using the medallion architecture pattern. At its core, the medallion architecture is a powerful data design pattern that brings structure and organization to your data lake, and we're implementing this using Apache Iceberg, which really shines in data lakehouse implementations.

Let me explain what we're trying to achieve here. We're building a system that processes incoming CSV files using Apache Spark, all within a lakehouse architecture that's organized into bronze, silver, and gold zones - think of it as progressive data refinement. What makes this particularly powerful is how we're orchestrating everything through AWS Step Functions and EventBridge, enabling real-time, event-driven processing.

One of the key features I'm particularly excited about is our support for incremental data processing. Instead of processing all data every time, we're only handling new or modified data, which makes everything more efficient. This isn't just about processing efficiency - it also optimizes our storage usage and makes queries faster. Plus, it makes data governance much simpler.

Apache Iceberg plays a crucial role here with its robust support for incremental processing. This allows us to optimize our entire medallion architecture, making our data operations more efficient and scalable. Think of it as building a high-performance engine where every component is designed to work together seamlessly, handling data changes intelligently while maintaining full data lineage and governance.

This approach gives organizations the best of both worlds - the flexibility and scalability of a data lake with the reliability and performance of a traditional data warehouse. And that's really what modern data architecture is all about: finding that sweet spot between flexibility, performance, and governance..


Slide 3 (Medallion Architecture Diagram):
"Let's break down the medallion architecture in detail:

Bronze Layer
This layer contains raw, unprocessed data ingested from various sources. It serves as a landing zone and retains data in its native format, ensuring that no information is lost during ingestion.

Silver Layer
Data in this layer is cleaned, transformed, and often enriched. It represents a more refined version of the data, ready for more complex transformations or aggregations.

Gold Layer
The gold layer consists of aggregated, business-level data ready for consumption by business intelligence (BI) tools, dashboards, and end-user applications. It is highly curated and structured to support specific use cases like analytics and reporting.


In short, in a Medallion architecture, the quality and structure of data improves as it passes through each layer. The bronze layer contains raw data, the silver layer contains cleansed and enriched data, and the gold layer contains data that is aggregated and ready to be analysed and integrated into business applications. 

In this lakehouse architecture, data is stored in a centralized S3 Tables with the scalability of a data lake and the performance of a data warehouse. S3 Tables offer built-in support for Apache Iceberg, which simplifies managing data lakes at scale while improving query performance and reducing costs. 


Slide 4 (Incremental Processing Benefits):

Let me explain why incremental processing is absolutely crucial in our medallion architecture especially when you're dealing with large-scale data operations.

Efficiency: Instead of reprocessing all data across each layer, incremental processing focuses only on new or changed data, reducing computation and resource usage. Only processes changed data
Timeliness: Incremental processing allows for quicker updates to downstream layers (silver and gold), ensuring that data consumers always have access to the most up-to-date information.
Scalability: By processing only the changed data, incremental processing helps to manage large-scale datasets effectively, making the architecture more scalable

These benefits compound each other, giving us an architecture that's not just more efficient, but also more responsive and future-proof. And in today's data-driven world, that's exactly what we need."


Slide 5 (Iceberg Table Anatomy):
"Understanding Iceberg's internal structure is crucial for effective implementation:

Let me walk you through the anatomy of an Iceberg table, because understanding this structure is key to leveraging Iceberg's full potential in your data architecture


First, think of the Catalog Layer as your air traffic controller. It keeps track of all tables and always knows where to find the latest metadata.

The Metadata Layer is our command center. This is where all the intelligence lives - managing snapshots, schemas, and maintaining file lists. Every time you make a change, Iceberg creates a new metadata file instead of modifying existing ones. So v1.metadata.json, v2.metadata.json, and so on form a chain of table history.

The Data Layer is exactly what it sounds like - where your actual data lives, typically in Parquet file

Next, we have snapshot management. Each snapshot is a point-in-time view of your table. Each write operation (insert, update, upsert, delete) in an Iceberg table creates a new snapshot. You can then use these snapshots for time travel—to go back in time and check the status of a table in the past.


This design makes Iceberg both powerful and reliable - you always know exactly what changed and when.

So When querying an Iceberg table: 
The query engine consults the catalog to find the latest metadata file
It reads the metadata file to get the manifest list location
It scans the manifest list to find relevant manifests
It uses manifests to identify which data files to read
Only the required files are read for the query



Using Spark SQL:
to query specific snapshot-id.

spark.sql(f"""
SELECT * FROM {CATALOG_NAME}.{DB_NAME}.{TABLE_NAME} VERSION AS OF {snapshot_id}
""")


Slide 6 

Let me explain the incremental data reading process with Iceberg snapshots. It really comes down to two key pieces: start-snapshot-id and end-snapshot-id.

First, how do we track changes? Iceberg provides special tables for inspecting and analyzing table state  With a simple SQL query:
SELECT * FROM {source_table}.history

This gives us everything we need: snapshot IDs, timestamps, and what operations happened - whether it was an append, delete, or overwrite.

For incremental processing, we need two snapshots:

The previous snapshot ID - which we store as a checkpoint in S3 or DynamoDB
The latest snapshot ID - which we get from our history query

spark.read.format("iceberg")
    .option("start-snapshot-id", 1000)  # last_snapshot
    .option("end-snapshot-id", 1234)    # latest_snapshot
    	.load(source_table)
  It returns all data changes between snapshots 1000 and 1234.  We then take these changes and MERGE them into our target table.

Slide 7

A MERGE operation is performed using the incremental data.

MERGE INTO silver_customer_table t
USING (
    SELECT *,
    ROW_NUMBER() OVER (
        PARTITION BY customer_id, email  -- Multiple columns as partition key
        ORDER BY processed_time DESC     -- Latest record based on processed_time
    ) as row_num
    FROM __temp_silver_updates
) s
ON t.customer_id = s.customer_id AND t.email = s.email  -- Composite key join
WHEN MATCHED AND s.row_num = 1 THEN UPDATE SET *
WHEN NOT MATCHED AND s.row_num = 1 THEN INSERT *



Let me break down this MERGE SQL 

PARTITION BY: Groups records by key columns
ROW_NUMBER(): Assigns 1 to latest record in each group
MATCHED: Updates existing records
NOT MATCHED: Inserts new records
row_num = 1: Ensures only latest version is applied

Slide 8

"Let me walk you through a practical example of how our MERGE operation works in the Silver layer.

Source Data (__temp_silver_updates):
customer_id | email           | name    | processed_time
1001        | john@test.com  | John    | 2024-04-01 10:00
1001        | john@test.com  | Johnny  | 2024-04-01 11:00  <- Latest
1002        | mary@test.com  | Mary    | 2024-04-01 10:00

Target Table (Before Merge):
customer_id | email           | name
1001        | john@test.com  | John
1003        | sam@test.com   | Sam

Result (After Merge):
customer_id | email           | name
1001        | john@test.com  | Johnny  <- Updated to latest
1002        | mary@test.com  | Mary    <- Inserted new
1003        | sam@test.com   | Sam     <- Unchanged

Let's start with our source data. We have some updates in a temporary table with multiple versions of customer records. Notice customer ID 1001 appears twice - once as 'John' and later updated to 'Johnny'.

In our target table, we currently have two records: John and Sam.

The result shows three key actions:

John's record gets updated to Johnny (taking the latest version)
Mary's record gets inserted as it's new
Sam's record stays unchanged

This single operation handles both updates and inserts while automatically managing duplicates by taking the latest version based on processed_time. Clean and efficient!"


slide 9

Let's talk about checkpoints - they're crucial for efficient data processing. Think of them as bookmarks that track where we left off.

Checkpoints serve two essential purposes:

Recovery: If a job fails, we can pick up right where we left off
Incremental Processing: We can easily identify and process only what's new

We use two types of checkpoints in our architecture:

In the Bronze layer, our checkpoint tracks when we last processed raw files using timestamps. This tells us exactly which new files need processing.

In the Silver layer, we track the last processed snapshot ID from the Bronze table. This lets us identify what data has changed since our last run.

Our checkpoint is stored as a simple JSON file in S3:

magine we have three files in our raw folder:

order1.csv (modified at 2:00)
order2.csv (modified at 3:00)
order3.csv (modified at 4:00)
If our checkpoint shows we last processed files up to 3:00, then only order3.csv gets processed because it's the only file modified after our checkpoint time.

This simple mechanism ensures we only process new data and can resume properly if anything fails.

Slide 10:

"Now let's see how checkpointing works in the Silver layer, which is slightly different from Bronz

Let's use a practical example. Say our Bronze table has three snapshots:

5618 (9:00 AM data)
5619 (10:00 AM data)
5620 (11:00 AM data)
If our checkpoint shows we last processed snapshot 5619, then we only process data from snapshot 5620. 

This ensures we're only processing new changes from Bronze to Silver, making our pipeline efficient and consistent.

Slide 11:

Let me introduce you to S3 Tables, which is really exciting new feature from AWS that takes Apache Iceberg to the next level
S3 Tables are fully managed Apache Iceberg tables in Amazon S3, that lets you store Apache Iceberg format tables natively in S3.
You get up to 10x higher transaction rates compared to regular S3 buckets right out of the box.
The automatic background compaction of your data files, queries can run up to 3 times faster. The system automatically consolidates your Parquet files for you - no manual maintenance required.

This is a game-changer for building high-performance data lakehouses with Apache Iceberg.

Slide 12:

Let me break down how S3 Tables are organized 

Think of it as a three-level hierarchy:

First, at the top, we have the Table Bucket. This is your catalog, and each table gets its own unique ARN. If you're familiar with databases, you can think of this as your database server.

Second level is what we call S3 Namespaces. These are basically folders within your bucket that help organize your tables. similar to schemas in a relational database or databases in the Glue Catalog.

Finally, we have the S3 Tables themselves, living within these namespaces. The actual data files for each table live here, organized in the Iceberg format.

slide 15:

Let me walk you through the AWS services we're using to build our data pipeline

First Glue Job - Raw to Bronze:

Reads new CSV files based on timestamp comparison
Writes data to Bronze S3 table
Updates bronze checkpoint with latest processed timestamp

Second Glue Job - Bronze to Silver:

Checks Silver checkpoint for last processed snapshot ID
Gets latest Bronze table snapshot ID from history
Processes data between these two snapshots
Writes incremental changes to Silver table
Updates Silver checkpoint with new snapshot ID

Step Functions orchestrates everything, making sure our pipeline runs smoothly and in the right order.

Finally, EventBridge Scheduler , Triggers our Step Functions workflow on a configurable schedule like hour, daily, or any interval you need

