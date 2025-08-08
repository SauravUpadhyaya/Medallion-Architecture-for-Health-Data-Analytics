# Databricks notebook source
# MAGIC %md
# MAGIC # Creating Data Lakehouse with Health Data
# MAGIC
# MAGIC ## Lakehouse Architecture Following Databricks Reference
# MAGIC
# MAGIC This project implements the **Medallion Architecture** with **Unity Catalog integration**:
# MAGIC
# MAGIC ```
# MAGIC Data Sources → Object Storage →  Bronze →  Silver →  Gold → Consumption Layer
# MAGIC (Apple Health)   (Workspace)    (Delta)    (Delta)    (Delta)  (Analytics/ML)
# MAGIC ```
# MAGIC
# MAGIC ### Architecture Components Build:
# MAGIC - ** Data Sources**: Apple Health XML exports 
# MAGIC - ** Object Storage**: Databricks workspace file system  
# MAGIC - ** Delta Tables**: ACID transactions across Bronze/Silver/Gold layers
# MAGIC - ** Unity Catalog**: Centralized metadata and governance (3-level namespace)
# MAGIC - ** Consumption Layer**: Data Marts, Analytics, ML-ready features

# COMMAND ----------

from IPython.display import Image, display

display(Image("/Workspace/Users/sauravupadhyaya123@gmail.com/Medallion_Architecture.png"))

'''
image source: https://www.kevinrchant.com/2024/05/03/the-great-number-of-workspaces-for-medallion-architecture-in-microsoft-fabric-debate/
'''


# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏛️ Setup Unity Catalog Structure
# MAGIC
# MAGIC Unity Catalog provides a 3-level namespace (catalog.schema.table) for organizing and securing data assets.
# MAGIC Let's create our catalog and schema structure for the Apple Health data.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Unity Catalog Setup
# MAGIC CREATE CATALOG IF NOT EXISTS health_data;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS health_data.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS health_data.silver; 
# MAGIC CREATE SCHEMA IF NOT EXISTS health_data.gold;
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS health_data.bronze.raw_files
# MAGIC COMMENT 'Volume for raw health XML data files';
# MAGIC
# MAGIC USE health_data.bronze;
# MAGIC
# MAGIC -- Display the Unity Catalog structure
# MAGIC SHOW SCHEMAS IN health_data;

# COMMAND ----------

# XML Processing (run this cell after SQL setup)
from pyspark.sql import SparkSession
import shutil, os, re

# Initialize SparkSession
spark = SparkSession.builder.getOrCreate()
print(f"✅ Spark {spark.version} ready for distributed processing!")

# Copy file to Unity Catalog Volume
source_file = "export.xml" 
volume_path = "/Volumes/health_data/bronze/raw_files/"
destination_file = "/Volumes/health_data/bronze/raw_files/"+source_file

if not os.path.exists(volume_path):
    os.makedirs(volume_path, exist_ok=True)

shutil.copy2(source_file, destination_file)

# COMMAND ----------

import xml.etree.ElementTree as ET
tree = ET.parse('export.xml')
root = tree.getroot()
print("Root tag:", root.tag)
print("Attributes:", root.attrib)

for child in root:
    print(child.tag, child.attrib)

# COMMAND ----------

import xml.etree.ElementTree as ET
import pandas as pd

data = []

for record in root.findall('.//Record'):
    data.append(record.attrib)

# Convert to DataFrame
df = pd.DataFrame(data)

# Display result
print(df.head(1))



# COMMAND ----------

# MAGIC %md
# MAGIC ## 🥉 Bronze Layer: Raw Data Ingestion
# MAGIC
# MAGIC **Implementing the first stage of Medallion Architecture with Unity Catalog**
# MAGIC
# MAGIC ```
# MAGIC Apple Health XML → Object Storage → Unity Catalog (health_data.bronze.apple_health)
# MAGIC ```
# MAGIC
# MAGIC - **Purpose**: Store raw data exactly as received from source systems
# MAGIC - **Format**: Delta Table with ACID transactions
# MAGIC - **Governance**: Unity Catalog metadata management
# MAGIC - **Features**: Audit trail, disaster recovery, time travel
# MAGIC

# COMMAND ----------

# Read XML from Unity Catalog Volume using Spark XML
df_parsed = spark.read \
    .option("rowTag", "Record") \
    .option("attributePrefix", "") \
    .option("valueTag", "_VALUE") \
    .option("inferSchema", "true") \
    .option("mode", "PERMISSIVE") \
    .format("xml") \
    .load(destination_file)

# Check total records parsed
total_records = df_parsed.count()
print(f"Total records: {total_records:,}")

# Preview the data
df_parsed.show(3, truncate=False)

# Create temp view for SQL queries
df_parsed.createOrReplaceTempView("raw_health_records")

# Display schema
print("\n📋 Final Schema:")
df_parsed.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT type, COUNT(*) FROM raw_health_records GROUP BY type
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------



# COMMAND ----------

# Transform raw data into Bronze Delta Table with metadata
# Create Bronze table with additional metadata columns

bronze_df = spark.sql(f"""
    SELECT
        *,
        current_timestamp() AS ingestion_timestamp,
        'export.xml' AS source_file,
        'bronze' AS layer,
        'Apple Health' AS source_system,
        {total_records} AS total_records_in_file
    FROM raw_health_records
""")

print(f"Bronze table will contain {bronze_df.count():,} records")

# Write to Delta Lake table in Unity Catalog
bronze_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("health_data.bronze.apple_health")

print("✅ Bronze Delta table created: health_data.bronze.apple_health")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Layer Metadata and Documentation
# MAGIC
# MAGIC Adding table properties and comments improves discoverability and governance in Unity Catalog.
# MAGIC

# COMMAND ----------

# Add table properties for optimization and governance
spark.sql("""
    ALTER TABLE health_data.bronze.apple_health 
    SET TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true',
        'description' = 'Raw Apple Health data ingested from XML export file'
    )
""")

# Add column descriptions for data governance
column_comments = [
    ("type", "Health metric type identifier from Apple Health"),
    ("value", "Raw measurement value"),
    ("unit", "Unit of measurement"),
    ("sourceName", "Source application or device name"),
    ("startDate", "Start timestamp of the health measurement"),
    ("endDate", "End timestamp of the health measurement"),
    ("creationDate", "When the record was created in Apple Health"),
    ("ingestion_timestamp", "When this record was ingested into the lakehouse"),
    ("source_file", "Original source file name"),
    ("layer", "Medallion architecture layer designation"),
    ("source_system", "Source system identifier"),
    ("total_records_in_file", "Total number of records in the source file")
]

for column, description in column_comments:
    spark.sql(f"""
        COMMENT ON COLUMN health_data.bronze.apple_health.{column} 
        IS '{description}'
    """)

print("✅ Table properties and column comments added")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Analyze Bronze layer with SQL
# MAGIC     SELECT 
# MAGIC         type, 
# MAGIC         COUNT(*) as record_count,
# MAGIC         COUNT(DISTINCT sourceName) as source_count,
# MAGIC         MIN(startDate) as earliest_date,
# MAGIC         MAX(endDate) as latest_date
# MAGIC     FROM health_data.bronze.apple_health
# MAGIC     GROUP BY type
# MAGIC     ORDER BY record_count DESC
# MAGIC     LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🥈 Silver Layer: Data Cleaning & Validation
# MAGIC
# MAGIC **Implementing the refinement stage of Medallion Architecture with Unity Catalog**
# MAGIC
# MAGIC ```
# MAGIC Bronze Delta Table → Transform → Silver Delta Table
# MAGIC health_data.bronze.apple_health → SQL transforms → health_data.silver.apple_health
# MAGIC ```
# MAGIC
# MAGIC - **Purpose**: Cleaned, validated, and enriched data
# MAGIC - **Features**: Schema enforcement, data quality checks, business logic
# MAGIC - **Format**: Optimized Delta Table with proper data types
# MAGIC - **Consumers**: Data scientists, ML engineers, advanced analytics

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Transform Bronze to Silver layer with data quality and business logic
# MAGIC CREATE OR REPLACE TABLE health_data.silver.apple_health AS
# MAGIC SELECT
# MAGIC     -- Core fields from bronze
# MAGIC     type,
# MAGIC     sourceName,
# MAGIC     value,
# MAGIC     unit,
# MAGIC     startDate,
# MAGIC     endDate,
# MAGIC     creationDate,
# MAGIC     
# MAGIC     -- Parse timestamps with timezone
# MAGIC     to_timestamp(startDate, 'yyyy-MM-dd HH:mm:ss ZZZ') AS start_timestamp,
# MAGIC     to_timestamp(endDate, 'yyyy-MM-dd HH:mm:ss ZZZ') AS end_timestamp,
# MAGIC     to_timestamp(creationDate, 'yyyy-MM-dd HH:mm:ss ZZZ') AS creation_timestamp,
# MAGIC     
# MAGIC     -- Validate and convert values
# MAGIC     CASE 
# MAGIC         WHEN value RLIKE '^[0-9]+\\.?[0-9]*$' THEN CAST(value AS DOUBLE)
# MAGIC         ELSE NULL
# MAGIC     END AS numeric_value,
# MAGIC     
# MAGIC     -- Business categorization
# MAGIC     CASE
# MAGIC         WHEN type LIKE '%HeartRate%' THEN 'Vitals'
# MAGIC         WHEN type LIKE '%StepCount%' THEN 'Activity'
# MAGIC         WHEN type LIKE '%BodyMass%' THEN 'Body'
# MAGIC         WHEN type LIKE '%Sleep%' THEN 'Sleep'
# MAGIC         ELSE 'Other'
# MAGIC     END AS metric_category,
# MAGIC     
# MAGIC     -- Clean metric name
# MAGIC     regexp_replace(type, 'HKQuantityTypeIdentifier', '') AS metric_name,
# MAGIC     
# MAGIC     -- Data quality flags
# MAGIC     CASE 
# MAGIC         WHEN value RLIKE '^[0-9]+\\.?[0-9]*$' THEN TRUE 
# MAGIC         ELSE FALSE
# MAGIC     END AS has_valid_value,
# MAGIC     
# MAGIC     CASE 
# MAGIC         WHEN to_timestamp(startDate, 'yyyy-MM-dd HH:mm:ss ZZZ') IS NOT NULL THEN TRUE
# MAGIC         ELSE FALSE
# MAGIC     END AS has_valid_dates,
# MAGIC     
# MAGIC     -- Silver layer metadata
# MAGIC     current_timestamp() AS processed_timestamp,
# MAGIC     'silver' AS layer
# MAGIC     
# MAGIC FROM health_data.bronze.apple_health
# MAGIC
# MAGIC -- Keep only quality records
# MAGIC WHERE 
# MAGIC     value RLIKE '^[0-9]+\\.?[0-9]*$' AND
# MAGIC     to_timestamp(startDate, 'yyyy-MM-dd HH:mm:ss ZZZ') IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     'Original' as table_source,
# MAGIC     COUNT(*) as record_count,
# MAGIC     COUNT(DISTINCT metric_category) as categories,
# MAGIC     MIN(processed_timestamp) as earliest_process_time,
# MAGIC     MAX(processed_timestamp) as latest_process_time
# MAGIC FROM health_data.silver.apple_health;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Layer Table Optimizations
# MAGIC
# MAGIC Delta Lake provides powerful optimizations for query performance and data management.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Optimize the Silver table for better query performance
# MAGIC OPTIMIZE health_data.silver.apple_health
# MAGIC ZORDER BY (start_timestamp, metric_category);
# MAGIC
# MAGIC -- Add table properties for governance
# MAGIC ALTER TABLE health_data.silver.apple_health 
# MAGIC SET TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true',
# MAGIC     'delta.columnMapping.mode' = 'name',
# MAGIC     'delta.minReaderVersion' = '2',
# MAGIC     'delta.minWriterVersion' = '5',
# MAGIC     'description' = 'Cleaned and validated Apple Health data with quality filters'
# MAGIC );

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT 
# MAGIC         metric_category,
# MAGIC         metric_name,
# MAGIC         COUNT(*) as record_count,
# MAGIC         ROUND(AVG(numeric_value), 2) as avg_value,
# MAGIC         ROUND(MIN(numeric_value), 2) as min_value,
# MAGIC         ROUND(MAX(numeric_value), 2) as max_value,
# MAGIC         SUM(CASE WHEN has_valid_value AND has_valid_dates THEN 1 ELSE 0 END) as valid_records,
# MAGIC         COUNT(DISTINCT DATE(start_timestamp)) as unique_days
# MAGIC     FROM health_data.silver.apple_health
# MAGIC     GROUP BY metric_category, metric_name
# MAGIC     ORDER BY metric_category, record_count DESC
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## 🥇 Gold Layer: Analytics-Ready Data
# MAGIC
# MAGIC **Implementing the consumption stage of Medallion Architecture with Unity Catalog**
# MAGIC
# MAGIC ```
# MAGIC Silver Delta Table → Transform → Gold Delta Table → Consumption Layer
# MAGIC health_data.silver.apple_health → SQL aggregations → health_data.gold.daily_health_summary
# MAGIC ```
# MAGIC
# MAGIC - **Purpose**: Business-ready aggregated data for analytics and ML
# MAGIC - **Features**: Daily summaries, KPIs, optimized for query performance  
# MAGIC - **Format**: Highly optimized Delta Table with business logic applied
# MAGIC - **Consumers**: Business analysts, BI tools, ML models, applications

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create Gold layer: Daily health summaries with business intelligence
# MAGIC CREATE OR REPLACE TABLE health_data.gold.daily_health_summary AS
# MAGIC
# MAGIC WITH daily_metrics AS (
# MAGIC     SELECT
# MAGIC         -- Date dimension
# MAGIC         DATE(start_timestamp) AS activity_date,
# MAGIC         metric_name,
# MAGIC         metric_category,
# MAGIC         unit,
# MAGIC         
# MAGIC         -- Core business aggregations
# MAGIC         SUM(numeric_value) AS total_value,
# MAGIC         AVG(numeric_value) AS avg_value,
# MAGIC         MIN(numeric_value) AS min_value,
# MAGIC         MAX(numeric_value) AS max_value,
# MAGIC         COUNT(*) AS measurement_count,
# MAGIC         
# MAGIC         -- Data quality metrics
# MAGIC         AVG(CASE WHEN has_valid_value AND has_valid_dates THEN 1.0 ELSE 0.0 END) AS quality_score,
# MAGIC         
# MAGIC         -- Business enrichment for analytics
# MAGIC         CASE WHEN DAYOFWEEK(start_timestamp) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,
# MAGIC         DAYOFWEEK(start_timestamp) AS day_of_week,
# MAGIC         MONTH(start_timestamp) AS month,
# MAGIC         YEAR(start_timestamp) AS year
# MAGIC     
# MAGIC     FROM health_data.silver.apple_health
# MAGIC     WHERE start_timestamp IS NOT NULL
# MAGIC     GROUP BY 
# MAGIC         DATE(start_timestamp),
# MAGIC         metric_name,
# MAGIC         metric_category,
# MAGIC         unit,
# MAGIC         CASE WHEN DAYOFWEEK(start_timestamp) IN (1, 7) THEN TRUE ELSE FALSE END,
# MAGIC         DAYOFWEEK(start_timestamp),
# MAGIC         MONTH(start_timestamp),
# MAGIC         YEAR(start_timestamp)
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     *,
# MAGIC     
# MAGIC     -- Evidence-based health scoring algorithm using medical guidelines
# MAGIC     CASE
# MAGIC         -- Heart Rate Scoring (based on American Heart Association guidelines)
# MAGIC         WHEN metric_name = 'HeartRate' THEN
# MAGIC             CASE 
# MAGIC                 WHEN avg_value BETWEEN 60 AND 100 THEN 95  -- Normal range per AHA
# MAGIC                 WHEN avg_value BETWEEN 50 AND 59 THEN 90   -- Athletic/very fit range
# MAGIC                 WHEN avg_value BETWEEN 40 AND 49 THEN 85   -- Elite athlete range
# MAGIC                 WHEN avg_value BETWEEN 101 AND 110 THEN 80 -- Slightly elevated
# MAGIC                 WHEN avg_value BETWEEN 111 AND 120 THEN 70 -- Moderately elevated
# MAGIC                 ELSE 60  -- Outside healthy ranges
# MAGIC             END
# MAGIC             
# MAGIC         -- Step Count Scoring (based on CDC and NIH research)
# MAGIC         WHEN metric_name = 'StepCount' THEN
# MAGIC             CASE 
# MAGIC                 WHEN avg_value >= 10000 THEN 95   -- CDC optimal recommendation
# MAGIC                 WHEN avg_value >= 8000 THEN 90    -- NIH minimum for health benefits
# MAGIC                 WHEN avg_value >= 6000 THEN 85    -- Beneficial for 60+ adults
# MAGIC                 WHEN avg_value >= 4000 THEN 75    -- Some health benefits
# MAGIC                 WHEN avg_value >= 2000 THEN 65    -- Minimal activity
# MAGIC                 ELSE 50  -- Sedentary lifestyle risk
# MAGIC             END
# MAGIC             
# MAGIC         -- Body Mass (weight in pounds) - contextual scoring
# MAGIC         WHEN metric_name = 'BodyMass' THEN
# MAGIC             CASE 
# MAGIC                 -- Assuming average height ~5'6" (67 inches), healthy BMI 18.5-24.9
# MAGIC                 WHEN avg_value BETWEEN 120 AND 165 THEN 90  -- Healthy BMI range
# MAGIC                 WHEN avg_value BETWEEN 110 AND 119 THEN 85  -- Lower healthy range
# MAGIC                 WHEN avg_value BETWEEN 166 AND 185 THEN 80  -- Overweight range
# MAGIC                 WHEN avg_value BETWEEN 100 AND 109 THEN 75  -- Underweight concern
# MAGIC                 WHEN avg_value BETWEEN 186 AND 220 THEN 70  -- Obesity class I
# MAGIC                 ELSE 60  -- Outside typical healthy ranges
# MAGIC             END
# MAGIC             
# MAGIC         -- Distance metrics (walking/running in miles)
# MAGIC         WHEN metric_name LIKE '%Distance%' THEN
# MAGIC             CASE 
# MAGIC                 WHEN avg_value >= 3.0 THEN 95     -- Excellent daily distance
# MAGIC                 WHEN avg_value >= 2.0 THEN 90     -- Good daily activity
# MAGIC                 WHEN avg_value >= 1.0 THEN 85     -- Moderate activity
# MAGIC                 WHEN avg_value >= 0.5 THEN 75     -- Some activity
# MAGIC                 ELSE 65  -- Limited mobility
# MAGIC             END
# MAGIC             
# MAGIC         -- Energy expenditure (calories)
# MAGIC         WHEN metric_name LIKE '%Energy%' THEN
# MAGIC             CASE 
# MAGIC                 WHEN avg_value >= 500 THEN 95     -- High activity level
# MAGIC                 WHEN avg_value >= 300 THEN 90     -- Moderate-high activity
# MAGIC                 WHEN avg_value >= 200 THEN 85     -- Moderate activity
# MAGIC                 WHEN avg_value >= 100 THEN 75     -- Light activity
# MAGIC                 ELSE 65  -- Minimal energy expenditure
# MAGIC             END
# MAGIC             
# MAGIC         -- Sleep duration (assuming hours)
# MAGIC         WHEN metric_name LIKE '%Sleep%' THEN
# MAGIC             CASE 
# MAGIC                 WHEN avg_value BETWEEN 7 AND 9 THEN 95   -- Optimal sleep per CDC
# MAGIC                 WHEN avg_value BETWEEN 6 AND 6.9 THEN 85 -- Acceptable minimum
# MAGIC                 WHEN avg_value BETWEEN 9.1 AND 10 THEN 85 -- Upper healthy range
# MAGIC                 WHEN avg_value BETWEEN 5 AND 5.9 THEN 70 -- Sleep deficient
# MAGIC                 ELSE 60  -- Severely inadequate sleep
# MAGIC             END
# MAGIC             
# MAGIC         -- Default for other metrics
# MAGIC         ELSE 80
# MAGIC     END AS health_score,
# MAGIC     
# MAGIC     -- Operational monitoring
# MAGIC     DATEDIFF(CURRENT_DATE(), activity_date) AS days_since_measurement,
# MAGIC     
# MAGIC     -- Trend categorization for business insights
# MAGIC     CASE
# MAGIC         WHEN avg_value > total_value / measurement_count * 1.2 THEN 'Significantly Above Average'
# MAGIC         WHEN avg_value > total_value / measurement_count * 1.1 THEN 'Above Average'
# MAGIC         WHEN avg_value < total_value / measurement_count * 0.8 THEN 'Significantly Below Average'
# MAGIC         WHEN avg_value < total_value / measurement_count * 0.9 THEN 'Below Average'
# MAGIC         ELSE 'Normal Range'
# MAGIC     END AS performance_trend,
# MAGIC     
# MAGIC     -- Gold layer metadata
# MAGIC     CURRENT_TIMESTAMP() AS processed_at,
# MAGIC     'gold' AS layer
# MAGIC     
# MAGIC FROM daily_metrics;

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC -- Optimize Gold table for analytical queries
# MAGIC OPTIMIZE health_data.gold.daily_health_summary
# MAGIC ZORDER BY (activity_date, metric_category);
# MAGIC
# MAGIC -- Add table properties for production governance
# MAGIC ALTER TABLE health_data.gold.daily_health_summary 
# MAGIC SET TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true',
# MAGIC     'delta.columnMapping.mode' = 'name',
# MAGIC     'description' = 'Daily health metrics aggregated from Apple Health data with evidence-based medical scoring'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top performing health metrics by medical scoring
# MAGIC SELECT 
# MAGIC     'Top Health Metrics by Medical Score' AS analysis_type,
# MAGIC     metric_name,
# MAGIC     metric_category,
# MAGIC     COUNT(*) AS total_days_tracked,
# MAGIC     ROUND(AVG(avg_value), 1) AS typical_daily_value,
# MAGIC     ROUND(AVG(health_score), 1) AS medical_score,
# MAGIC     CASE 
# MAGIC         WHEN AVG(health_score) >= 90 THEN 'Excellent Health'
# MAGIC         WHEN AVG(health_score) >= 80 THEN 'Good Health' 
# MAGIC         WHEN AVG(health_score) >= 70 THEN 'Fair Health'
# MAGIC         ELSE 'Needs Improvement'
# MAGIC     END AS health_grade,
# MAGIC     COUNT(CASE WHEN performance_trend = 'Significantly Above Average' THEN 1 END) AS exceptional_days
# MAGIC FROM health_data.gold.daily_health_summary
# MAGIC WHERE activity_date >= '2024-01-01'
# MAGIC GROUP BY metric_name, metric_category
# MAGIC HAVING COUNT(*) >= 30  -- At least 30 days of data
# MAGIC ORDER BY medical_score DESC, total_days_tracked DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔺 Delta Lake Advanced Features
# MAGIC
# MAGIC **Demonstrating Enterprise Lakehouse Capabilities**
# MAGIC
# MAGIC Delta Lake provides the reliability and performance features that make the lakehouse architecture possible:
# MAGIC
# MAGIC ### 🕰️ Time Travel
# MAGIC - Query any historical version of your data
# MAGIC - Rollback bad deployments or data corruption
# MAGIC - Audit trail for compliance and debugging
# MAGIC
# MAGIC ### 🔄 Schema Evolution  
# MAGIC - Add columns without breaking existing pipelines
# MAGIC - Backward compatibility for all consumers
# MAGIC - Gradual schema migration strategies
# MAGIC
# MAGIC ### ⚡ Performance Optimization
# MAGIC - Z-ordering for optimal query performance
# MAGIC - Automatic file compaction and optimization
# MAGIC - Efficient metadata management

# COMMAND ----------

# MAGIC %sql
# MAGIC --Demonstrate Delta Lake time travel capabilities with SQL
# MAGIC DESCRIBE HISTORY health_data.gold.daily_health_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC --Query a specific version using time travel
# MAGIC SELECT COUNT(*) as record_count, 'Version 0' as version
# MAGIC FROM health_data.gold.daily_health_summary VERSION AS OF 0
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT COUNT(*) as record_count, 'Current Version' as version
# MAGIC FROM health_data.gold.daily_health_summary
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Data Pipeline Performance Summary
# MAGIC
# MAGIC The medallion architecture transforms raw iPhone health data through **Bronze**, **Silver**, and **Gold** layers. This analysis shows how many records exist at each stage and demonstrates the efficiency of our data cleaning and aggregation processes in creating analytics-ready daily health summaries.

# COMMAND ----------

import matplotlib.pyplot as plt
import pandas as pd

# Get record counts from each layer using correct table names
bronze_count = spark.sql("SELECT COUNT(*) as count FROM health_data.bronze.apple_health").collect()[0]['count']
silver_count = spark.sql("SELECT COUNT(*) as count FROM health_data.silver.apple_health").collect()[0]['count']
gold_count = spark.sql("SELECT COUNT(*) as count FROM health_data.gold.daily_health_summary").collect()[0]['count']

# Create summary DataFrame
pipeline_data = pd.DataFrame({
    'Layer': ['Bronze\n(Raw)', 'Silver\n(Cleaned)', 'Gold\n(Aggregated)'],
    'Record_Count': [bronze_count, silver_count, gold_count],
    'Colors': ['#CD7F32', '#C0C0C0', '#FFD700']  # Bronze, Silver, Gold colors
})

# Calculate data reduction percentages
bronze_to_silver_reduction = ((bronze_count - silver_count) / bronze_count) * 100
silver_to_gold_reduction = ((silver_count - gold_count) / silver_count) * 100
total_reduction = ((bronze_count - gold_count) / bronze_count) * 100

print("="*60)
print("DATA PIPELINE PROCESSING SUMMARY")
print("="*60)

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
fig.suptitle('Health Data Pipeline: Bronze → Silver → Gold', fontsize=16, fontweight='bold')

# 1. Bar Chart showing record counts
bars = ax1.bar(pipeline_data['Layer'], pipeline_data['Record_Count'], 
               color=pipeline_data['Colors'], alpha=0.8, edgecolor='black', linewidth=2)

# Add value labels on bars
for bar, count in zip(bars, pipeline_data['Record_Count']):
    height = bar.get_height()
    ax1.text(bar.get_x() + bar.get_width()/2., height,
             f'{count:,}', ha='center', va='bottom', fontweight='bold', fontsize=12)

ax1.set_title('Record Counts by Pipeline Layer', fontweight='bold', fontsize=14)
ax1.set_ylabel('Number of Records', fontweight='bold')
ax1.grid(True, alpha=0.3, axis='y')

# Format y-axis with commas
ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:,.0f}'))

# 2. Data Flow Visualization
ax2.axis('off')

# Create flow diagram without emojis
flow_text = f"""
DATA TRANSFORMATION FLOW
{'='*30}

BRONZE LAYER (Raw Data)
   Records: {bronze_count:,}
   Source: iPhone Health Export XML
   Status: Raw, unprocessed data

        |  Data Cleaning & Validation
        v  Removed: {bronze_count - silver_count:,} records ({bronze_to_silver_reduction:.1f}%)

SILVER LAYER (Cleaned Data)  
   Records: {silver_count:,}
   Process: Cleaned, validated, standardized
   Quality: High-quality individual records

        |  Aggregation & Summarization  
        v  Reduced: {silver_count - gold_count:,} records ({silver_to_gold_reduction:.1f}%)

GOLD LAYER (Analytics-Ready)
   Records: {gold_count:,}
   Format: Daily summaries & health scores
   Purpose: Dashboard & analytics

OVERALL DATA REDUCTION: {total_reduction:.1f}%
({bronze_count:,} -> {gold_count:,} records)
"""

ax2.text(0.05, 0.95, flow_text, transform=ax2.transAxes, fontsize=11,
         verticalalignment='top',
         bbox=dict(boxstyle="round,pad=0.8", facecolor='lightblue', alpha=0.1))

plt.tight_layout()
plt.show()

# Print detailed summary
print(f"PIPELINE STATISTICS:")
print(f"   • Bronze Layer: {bronze_count:,} records")
print(f"   • Silver Layer: {silver_count:,} records ({bronze_to_silver_reduction:.1f}% reduction)")
print(f"   • Gold Layer: {gold_count:,} records ({silver_to_gold_reduction:.1f}% further reduction)")
print(f"\nTRANSFORMATION EFFICIENCY:")
print(f"   • Data cleaning removed: {bronze_count - silver_count:,} invalid/duplicate records")
print(f"   • Aggregation condensed: {silver_count - gold_count:,} records into daily summaries")
print(f"   • Overall compression ratio: {bronze_count/gold_count:.1f}:1")
print(f"   • Final dataset size: {total_reduction:.1f}% smaller than raw input")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Health Data Analytics & Visualizations
# MAGIC
# MAGIC Now we can use our Gold layer data to create insightful visualizations and analyses.

# COMMAND ----------

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

# Simple styling
plt.style.use('default')
COLORS = ['#2E86AB', '#A23B72', '#F18F01', '#C73E1D', '#85C1E9', '#90EE90']

# Get clean health data
health_summary = spark.sql("""
    SELECT activity_date, metric_name, metric_category, avg_value, health_score, 
           is_weekend, measurement_count, performance_trend, month
    FROM health_data.gold.daily_health_summary
    WHERE activity_date >= '2024-01-01' AND metric_category IN ('Vitals', 'Activity', 'Body')
      AND health_score IS NOT NULL AND avg_value IS NOT NULL
    ORDER BY activity_date
""").toPandas()

if len(health_summary) > 0:
    health_summary = health_summary.dropna(subset=['health_score', 'avg_value'])
    health_summary['activity_date'] = pd.to_datetime(health_summary['activity_date'])
    
    fig, axes = plt.subplots(2, 3, figsize=(18, 12))
    fig.suptitle('Health Analytics Dashboard 2024', fontsize=20, fontweight='bold', y=0.98)
    
    categories = health_summary['metric_category'].unique()
    
    # 1. Monthly Health Trends
    ax = axes[0, 0]
    for i, cat in enumerate(categories):
        monthly = health_summary[health_summary['metric_category'] == cat].groupby('month')['health_score'].mean()
        if len(monthly) > 0:
            ax.plot(monthly.index, monthly.values, marker='o', linewidth=2, 
                   label=cat, color=COLORS[i])
    ax.set_title('Monthly Health Trends', fontweight='bold')
    ax.set_ylabel('Health Score')
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    # 2. Weekend vs Weekday
    ax = axes[0, 1]
    weekend_data, weekday_data, cat_names = [], [], []
    for cat in categories:
        cat_data = health_summary[health_summary['metric_category'] == cat]
        weekend = cat_data[cat_data['is_weekend'] == True]['health_score'].mean()
        weekday = cat_data[cat_data['is_weekend'] == False]['health_score'].mean()
        if not pd.isna(weekend) and not pd.isna(weekday):
            weekend_data.append(weekend)
            weekday_data.append(weekday)
            cat_names.append(cat)
    
    if cat_names:
        x = np.arange(len(cat_names))
        ax.bar(x - 0.2, weekday_data, 0.4, label='Weekday', color=COLORS[0], alpha=0.8)
        ax.bar(x + 0.2, weekend_data, 0.4, label='Weekend', color=COLORS[1], alpha=0.8)
        ax.set_title('Weekend vs Weekday', fontweight='bold')
        ax.set_xticks(x)
        ax.set_xticklabels(cat_names)
        ax.legend()
        ax.grid(True, alpha=0.3, axis='y')
    
    # 3. Heart Rate Distribution
    ax = axes[0, 2]
    hr_data = health_summary[health_summary['metric_name'] == 'HeartRate']['avg_value'].dropna()
    if len(hr_data) > 0:
        ax.hist(hr_data, bins=15, alpha=0.7, color=COLORS[2], edgecolor='black')
        ax.axvline(hr_data.mean(), color='red', linestyle='--', linewidth=2, 
                  label=f'Mean: {hr_data.mean():.0f} bpm')
        ax.set_title('Heart Rate Distribution', fontweight='bold')
        ax.set_xlabel('Heart Rate (bpm)')
        ax.legend()
        ax.grid(True, alpha=0.3, axis='y')
    else:
        ax.text(0.5, 0.5, 'No Heart Rate Data', ha='center', va='center', transform=ax.transAxes)
    
    # 4. Health Score Distribution by Category
    ax = axes[1, 0]
    cat_scores = [health_summary[health_summary['metric_category'] == cat]['health_score'].dropna() 
                  for cat in categories]
    cat_scores = [scores for scores in cat_scores if len(scores) > 0]
    
    if cat_scores:
        bp = ax.boxplot(cat_scores, labels=categories[:len(cat_scores)], patch_artist=True)
        for patch, color in zip(bp['boxes'], COLORS):
            patch.set_facecolor(color)
            patch.set_alpha(0.7)
        ax.set_title('Score Distribution by Category', fontweight='bold')
        ax.set_ylabel('Health Score')
        ax.grid(True, alpha=0.3, axis='y')
    
    # 5. Weekly Activity Trends
    ax = axes[1, 1]
    activity_data = health_summary[health_summary['metric_category'] == 'Activity'].copy()
    if len(activity_data) > 0:
        activity_data['week'] = activity_data['activity_date'].dt.isocalendar().week
        weekly = activity_data.groupby('week')['health_score'].mean().dropna()
        if len(weekly) > 1:
            ax.plot(weekly.index, weekly.values, color=COLORS[0], linewidth=2, marker='o')
            ax.set_title('Weekly Activity Trends', fontweight='bold')
            ax.set_xlabel('Week')
            ax.set_ylabel('Health Score')
            ax.grid(True, alpha=0.3)
        else:
            ax.text(0.5, 0.5, 'Insufficient Activity Data', ha='center', va='center', transform=ax.transAxes)
    else:
        ax.text(0.5, 0.5, 'No Activity Data', ha='center', va='center', transform=ax.transAxes)
    
    # 6. Summary Stats
    ax = axes[1, 2]
    ax.axis('off')
    
    total_days = len(health_summary['activity_date'].unique())
    avg_score = health_summary['health_score'].mean()
    best_cat = health_summary.groupby('metric_category')['health_score'].mean().idxmax()
    hr_avg = health_summary[health_summary['metric_name'] == 'HeartRate']['avg_value'].mean()
    
    summary = f"""HEALTH SUMMARY 2024
{'='*20}

Days Tracked: {total_days}
Records: {len(health_summary):,}
Avg Score: {avg_score:.1f}/100
Best Category: {best_cat}
Avg Heart Rate: {hr_avg:.0f} bpm

Performance: {'Excellent' if avg_score >= 85 else 'Good' if avg_score >= 75 else 'Fair'}
Coverage: {(total_days/365)*100:.0f}% of year"""
    
    ax.text(0.05, 0.95, summary, transform=ax.transAxes, fontsize=11, 
           verticalalignment='top', fontfamily='monospace',
           bbox=dict(boxstyle="round,pad=0.5", facecolor='lightgray', alpha=0.8))
    
    plt.tight_layout()
    plt.subplots_adjust(top=0.92)
    plt.show()
    
    print(f"Dashboard complete! Analyzed {len(health_summary):,} records from {total_days} days.")

else:
    print("No health data found.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Advanced SQL Analytics on the Gold Layer
# MAGIC
# MAGIC Demonstrate powerful SQL analytics capabilities on our well-structured Gold layer data.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Executive Health Summary: Key insights at a glance
# MAGIC SELECT 
# MAGIC     '🏥 OVERALL HEALTH REPORT' AS section,
# MAGIC     '' AS metric,
# MAGIC     '' AS value,
# MAGIC     '' AS insight
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     'Health Score Overview',
# MAGIC     'Overall Health Score',
# MAGIC     ROUND(AVG(health_score), 1) || '/100',
# MAGIC     CASE 
# MAGIC         WHEN AVG(health_score) >= 90 THEN '🟢 Excellent - Following medical guidelines'
# MAGIC         WHEN AVG(health_score) >= 80 THEN '🟡 Good - Room for minor improvements'
# MAGIC         WHEN AVG(health_score) >= 70 THEN '🟠 Fair - Consider lifestyle changes'
# MAGIC         ELSE '🔴 Needs attention - Consult healthcare provider'
# MAGIC     END
# MAGIC FROM health_data.gold.daily_health_summary
# MAGIC WHERE activity_date >= DATE_SUB(CURRENT_DATE(), 90)
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     'Activity Performance',
# MAGIC     'Step Count Performance',
# MAGIC     ROUND(AVG(CASE WHEN metric_name = 'StepCount' THEN health_score END), 1) || '/100',
# MAGIC     CASE 
# MAGIC         WHEN AVG(CASE WHEN metric_name = 'StepCount' THEN avg_value END) >= 10000 THEN '🟢 Meeting CDC recommendations (10k+ steps)'
# MAGIC         WHEN AVG(CASE WHEN metric_name = 'StepCount' THEN avg_value END) >= 8000 THEN '🟡 Good activity level (8k+ steps)'
# MAGIC         WHEN AVG(CASE WHEN metric_name = 'StepCount' THEN avg_value END) >= 6000 THEN '🟠 Moderate activity (6k+ steps)'
# MAGIC         ELSE '🔴 Below recommended activity levels'
# MAGIC     END
# MAGIC FROM health_data.gold.daily_health_summary
# MAGIC WHERE metric_name = 'StepCount' AND activity_date >= DATE_SUB(CURRENT_DATE(), 90)
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     'Cardiovascular Health',
# MAGIC     'Heart Rate Assessment',
# MAGIC     ROUND(AVG(CASE WHEN metric_name = 'HeartRate' THEN health_score END), 1) || '/100',
# MAGIC     CASE 
# MAGIC         WHEN AVG(CASE WHEN metric_name = 'HeartRate' THEN avg_value END) BETWEEN 60 AND 80 THEN '🟢 Optimal heart rate (AHA guidelines)'
# MAGIC         WHEN AVG(CASE WHEN metric_name = 'HeartRate' THEN avg_value END) BETWEEN 50 AND 100 THEN '🟡 Normal range for your fitness level'
# MAGIC         ELSE '🟠 Consider discussing with healthcare provider'
# MAGIC     END
# MAGIC FROM health_data.gold.daily_health_summary
# MAGIC WHERE metric_name = 'HeartRate' AND activity_date >= DATE_SUB(CURRENT_DATE(), 90)
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     'Lifestyle Patterns',
# MAGIC     'Weekend vs Weekday',
# MAGIC     ROUND(
# MAGIC         AVG(CASE WHEN is_weekend THEN health_score END) - 
# MAGIC         AVG(CASE WHEN NOT is_weekend THEN health_score END), 1
# MAGIC     ) || ' point difference',
# MAGIC     CASE 
# MAGIC         WHEN AVG(CASE WHEN is_weekend THEN health_score END) > AVG(CASE WHEN NOT is_weekend THEN health_score END) + 2 
# MAGIC         THEN '🟢 More active on weekends - good work-life balance'
# MAGIC         WHEN AVG(CASE WHEN NOT is_weekend THEN health_score END) > AVG(CASE WHEN is_weekend THEN health_score END) + 2 
# MAGIC         THEN '🟡 More active on weekdays - consider weekend activities'
# MAGIC         ELSE '🟢 Consistent activity pattern throughout week'
# MAGIC     END
# MAGIC FROM health_data.gold.daily_health_summary
# MAGIC WHERE activity_date >= DATE_SUB(CURRENT_DATE(), 90)
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     'Data Quality',
# MAGIC     'Tracking Consistency',
# MAGIC     COUNT(DISTINCT activity_date) || ' days in last 90',
# MAGIC     CASE 
# MAGIC         WHEN COUNT(DISTINCT activity_date) >= 75 THEN '🟢 Excellent tracking consistency'
# MAGIC         WHEN COUNT(DISTINCT activity_date) >= 60 THEN '🟡 Good tracking consistency'
# MAGIC         WHEN COUNT(DISTINCT activity_date) >= 30 THEN '🟠 Moderate tracking - room for improvement'
# MAGIC         ELSE '🔴 Limited tracking - consider more consistent monitoring'
# MAGIC     END
# MAGIC FROM health_data.gold.daily_health_summary
# MAGIC WHERE activity_date >= DATE_SUB(CURRENT_DATE(), 90)
# MAGIC
# MAGIC ORDER BY 
# MAGIC     CASE section 
# MAGIC         WHEN '🏥 OVERALL HEALTH REPORT' THEN 1
# MAGIC         WHEN 'Health Score Overview' THEN 2
# MAGIC         WHEN 'Activity Performance' THEN 3
# MAGIC         WHEN 'Cardiovascular Health' THEN 4
# MAGIC         WHEN 'Lifestyle Patterns' THEN 5
# MAGIC         WHEN 'Data Quality' THEN 6
# MAGIC     END;

# COMMAND ----------

spark.table("bronze.apple_health").printSchema()




# COMMAND ----------

spark.table("silver.apple_health").printSchema()



# COMMAND ----------

spark.table("gold.daily_health_summary").printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Bronze
# MAGIC DESCRIBE TABLE bronze.apple_health;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Silver
# MAGIC DESCRIBE TABLE silver.apple_health;
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Gold
# MAGIC DESCRIBE TABLE gold.daily_health_summary;

# COMMAND ----------

df_gold = spark.table("gold.daily_health_summary")
# df_gold.show(10, truncate=False)
pdf = df_gold.limit(10).toPandas()
display(pdf)
 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   year,
# MAGIC   weekofyear(activity_date) AS week_num,
# MAGIC   ROUND(AVG(avg_value), 1) AS avg_resting_hr
# MAGIC FROM gold.daily_health_summary
# MAGIC WHERE metric_name = 'WalkingSpeed'
# MAGIC GROUP BY year, weekofyear(activity_date)
# MAGIC ORDER BY year, week_num;