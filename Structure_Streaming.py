


from pyspark.sql.functions import *
from pyspark.sql.types import *


# Read all files with name starting with "bank" and ending with ".json" from the FileStore folder and create a static DataFrame
get_sch_bank = spark.read.json("dbfs:/FileStore/bank*.json")

# Get the schema of the static DataFrame
bank_static_sch = get_sch_bank.schema

# Read all files with name starting with "bank" and ending with ".json" from the FileStore folder as a streaming DataFrame
df_ss_bronze_bank = spark.readStream \
  .schema(bank_static_sch) \                # Use the schema obtained earlier
  .format("json") \                         # Set the format to "json"
  .option("header", "true") \               # Enable header inference 
  .option("maxFilesPerTrigger", "1") \      # Process only one file per trigger
  .load("dbfs:/FileStore/bank*.json")       # Reading the data from this path

# Check if the DataFrame is streaming
df_ss_bronze_bank.isStreaming

# Read all files with name starting with "credit_card" and ending with ".json" from the FileStore folder and create a static DataFrame
get_sch_credit_card = spark.read.json("dbfs:/FileStore/credit_card*.json")

# Get the schema of the static DataFrame
credit_card_static_sch = get_sch_credit_card.schema

# Read all files with name starting with "credit_card" and ending with ".json" from the FileStore folder as a streaming DataFrame
df_ss_bronze_credit_card = spark.readStream \
  .schema(credit_card_static_sch) \            # Use the schema obtained earlier
  .format("json") \                            # Set the format to "json"
  .option("header", "true") \                  # Enable header inference 
  .option("maxFilesPerTrigger", "1") \         # Process only one file per trigger
  .load("dbfs:/FileStore/credit_card*.json")   # Reading the data from this path

# Check if the DataFrame is streaming
df_ss_bronze_credit_card.isStreaming

#Since I'm working with a Lakehouse/Medallion Architecture, I'll write the bronze layer without any modifications in the data.
df_ss_bronze_bank \
  .writeStream \                                                  
  .format("delta") \                                                # Writting in Delta Format 
  .outputMode("append") \                                           # Appending the values
  .option("checkpointLocation", "dbfs:/FileStore/_checkpoint") \    # Using this path as a Checkpoint
  .start("dbfs:/FileStore/bronze/bank")                             # Writing into this path 

#Since I'm working with a Lakehouse/Medallion Architecture, I'll write the bronze layer without any modifications in the data.
df_ss_bronze_credit_card \
  .writeStream \                                                    
  .format("delta") \                                               # Writting in Delta Format 
  .outputMode("append") \                                          # Appending the values
  .option("checkpointLocation", "dbfs:/FileStore/_checkpoint") \   # Using this path as a Checkpoint
  .start("dbfs:/FileStore/bronze/credit_card")                     # Writing into this path 


#Creating a streaming DataFrames to read data from a Delta table

df_ss_silver_bank = spark.readStream
                         .option("failOnDataLoss", "false")  #Configuring the stream to continue running even if data is lost or truncated from the source
                         .format("delta")                    #Specifying the file format of the data source
                         .load("dbfs:/FileStore/bronze/bank")#Specifying the source path to read the data from
      
df_ss_silver_credit_card = spark.readStream
                          .option("failOnDataLoss", "false") #Configuring the stream to continue running even if data is lost or truncated from the source
                          .format("delta")                   #Specifying the file format of the data source
                          .load("dbfs:/FileStore/bronze/credit_card") #Specifying the source path to read the data from




#Casting the column dt_current_timestamp to transform to a date readable format
transf_silver_bank = df_ss_silver_bank.withColumn("dt_current_timestamp", from_unixtime(col("dt_current_timestamp")/1000).cast(DateType()))
transf_silver_credit_card = df_ss_silver_credit_card.withColumn("dt_current_timestamp", from_unixtime(col("dt_current_timestamp")/1000).cast(DateType())) 

#Selecting only the columns that I'm interested in

transf_silver_bank = transf_silver_bank.select(col("user_id"),
                                               col("account_number"),
                                               col("bank_name"),
                                               col("dt_current_timestamp").alias("Data_criacao_conta"),
                                               col("iban")
                                              )


transf_silver_credit_card = transf_silver_credit_card.select(col("user_id"),
                                                             col("credit_card_number"),
                                                             col("credit_card_type"),
                                                             col("credit_card_expiry_date"),
                                                             col("dt_current_timestamp").alias("Data_criacao_cartao"),
                                                            )

#Writing in the silver layer

transf_silver_bank.writeStream \                                                     
                  .format("delta") \                                                      # specifies that the format of the output is Delta Lake
                  .option("checkpointLocation", "dbfs:/FileStore/_checkpoint/silver") \ # specifies the location where the checkpoint information will be stored
                  .start("dbfs:/FileStore/silver/bank") # starts the write stream and specifies the location where the transformed data will be written to


transf_silver_credit_card.writeStream \
                         .format("delta") \
                         .option("checkpointLocation", "dbfs:/FileStore/_checkpoint/silver/credit_card") \
                         .start("dbfs:/FileStore/silver/credit_card")


df_ss_gold_bank = spark.readStream \
                       .option("failOnDataLoss", "false") \  #Configuring the stream to continue running even if data is lost or truncated from the source
                       .format("delta") \                    #Specifying the file format of the data source  
                       .load("dbfs:/FileStore/silver/bank")  #Specifying the source path to read the data from

df_ss_gold_credit_card = spark.readStream \
                              .option("failOnDataLoss", "false")\
                              .format("delta")\
                              .load("dbfs:/FileStore/silver/credit_card")

#joining the 2 dataframes
transf_gold_account = df_ss_gold_bank.join(df_ss_gold_credit_card, df_ss_gold_bank.user_id == df_ss_gold_credit_card.user_id, "inner") \
                                     .drop(df_ss_gold_credit_card.user_id)



#Selecting only the columns that I'm interested in
transf_gold_account = transf_gold_account.select(
                                                  col("user_id").alias("User_Id"),
                                                  col("account_number").alias("Account_Number"),
                                                  col("bank_name").alias("Bank_Name"),
                                                  col("credit_card_number").alias("Credit_x
                                                  col("Data_criacao_conta").alias("Account_Date_Creation"),
                                                  col("Data_criacao_cartao").alias("Credit_Card_Creation")


#Writing in the gold layer 
transf_gold_account \
  .writeStream \
  .format("delta") \                                                   # specifies that the format of the output is Delta Lake
  .outputMode("append") \                                              # Appending the values
  .option("checkpointLocation", "dbfs:/FileStore/_checkpoint/gold") \  # Using this path as a Checkpoint
  .start("dbfs:/FileStore/gold/account")                               # Writing into this path 
 