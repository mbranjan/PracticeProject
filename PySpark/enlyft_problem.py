from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, regexp_replace, split, size, when

spark = (SparkSession
         .builder
         .appName("Enlyft")
         .master("local[*]")
         .getOrCreate())

companies_df = (spark.read.parquet("resources/companies.parquet")
                .withColumn("cleaned_url", regexp_replace(col("canonical_url"), "/$", ""))
                .withColumn("part_url", split(col("cleaned_url"), "/")[size(split(col("cleaned_url"), "/")) - 1])
                )

people_raw_df = (spark.read.parquet("resources/people_data.parquet")
                 .select(col("name").alias("person_name"), col("url").alias("person_url"),
                         "member_experience_collection")
                 )

exploded_df = (people_raw_df
               .withColumn("member_experience", explode("member_experience_collection"))
               .drop("member_experience_collection")
               )
people_data_df = (exploded_df
                  .withColumn("company_name", col("member_experience.company_name"))
                  .withColumn("company_domain", col("member_experience.company_url"))
                  .withColumn("job_title", col("member_experience.title"))
                  .withColumn("job_start_date", col("member_experience.date_from"))
                  .withColumn("job_end_date", col("member_experience.date_to"))
                  .withColumn("duration", col("member_experience.duration"))
                  .withColumn("last_updated", col("member_experience.last_updated"))
                  .withColumn("location", col("member_experience.location"))
                  .withColumn("enlyft_id", when(col("company_domain").isNull(), col("company_name"))
                              .otherwise(
    split(col("company_domain"), "/")[size(split(col("company_domain"), "/")) - 1]))
                  .drop("member_experience")
                  )

final_df = (people_data_df
            .join(companies_df,
                  (people_data_df["enlyft_id"] == companies_df["part_url"]) |
                  (people_data_df["enlyft_id"] == companies_df["name"]), "inner")
            .drop("part_url", "cleaned_url")
            )

print(final_df.count())
final_df.show(truncate=False)
final_df.write.mode("overwrite").parquet("output/result")
