from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# S3 paths
s3_source_path = "s3://adzuna-etl-project-g2/adzuna/data_engineer_ca/"
s3_output_path = "s3://adzuna-etl-project-g2/transformed_data/"

# Read raw JSON
source_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    connection_options={
        "paths": [s3_source_path]
    }
)

# Convert to DataFrame
jobs_df = source_dyf.toDF()

# Select & rename fields (no explode!)
jobs_df = jobs_df.select(
    col("id").alias("job_id"),
    col("title").alias("job_title"),
    col("location.display_name").alias("job_location"),
    col("company.display_name").alias("job_company"),
    col("category.label").alias("job_category"),
    col("description").alias("job_description"),
    col("redirect_url").alias("job_url"),
    col("created").cast("timestamp").alias("job_created")
)

# Deduplicate
jobs_df = jobs_df.dropDuplicates(["job_id"])

# Write Parquet
jobs_df.write.mode("append").parquet(s3_output_path)

job.commit()