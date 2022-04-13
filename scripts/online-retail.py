from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
 
schema_online_retail = StructType([
	StructField("InvoiceNo",  StringType(),  True),
	StructField("StockCode",  StringType(),  True),
	StructField("Description",  StringType(),  True),
	StructField("Quantity",  IntegerType(),  True),
	StructField("InvoiceDate",  StringType(),  True),
	StructField("UnitPrice",  StringType(),  True),
	StructField("CustomerID",  IntegerType(),  True),
	StructField("Country",  StringType(),  True),
])
# Questions QA 1

def question_1_qa_InvoiceNo (df):
	df = df.withColumn('qa_InvoiceNo', (
		F.when(F.col("InvoiceNo").startswith("C"), "Cancelled")
		.when((F.col("InvoiceNo").rlike("^[0-9]*$")), "Effective")
		.otherwise("Unknown")
	))

# Questions QA 2

def question_2_qa_StockCode (df):
	df = df.withColumn("qa_StockCode", (
		F.when(~F.col("StockCode").rlike("([0-9a-zA-Z]{5})"), "Missing")
		.otherwise("Effective")
	))
	print(df.groupBy("qa_StockCode").count().show())

# Questions QA 3

def question_3_qa_Description (df):
	df = df.withColumn("qa_Description", (
		F.when(F.col("Description").isNull(), "Missing")
		.when(F.col("Description") == "", "Missing")
		.otherwise("Effective")
		))
	print(df.groupBy("qa_Description").count().show())

# Questions QA 4

def question_4_qa_InvoiceDate (df):
	df = df.withColumn("InvoiceDate",(
		F.to_timestamp(F.col("InvoiceDate"), "d/M/yyyy H:m")
	))
	print(df.printSchema())

# Questions QA 5

def question_5_qa_UnitPrice (df):
	df = df.withColumn("UnitPrice", F.regexp_replace("UnitPrice", ",",".").cast("float"))


# Questions QA 6

def question_6_qa_CustomerID(df):
	df = df.withColumn("qa_CustomerID", (
	F.when(~F.col("CustomerID").rlike("([0-9a-zA-Z]{5})"), "Failure").otherwise("Effective")))
	print(df.groupBy("qa_CustomerID").count().distinct().orderBy("qa_CustomerID", ascending=False).show())

# Questions QA 7

def question_7_qa_Country(df):
	df = df.withColumn("qa_Country", (
	F.when(F.col("Country").isNull(), "Missing")
	 .when(F.col("Country") == "", "Missing")
	 .otherwise("Effective")))
	print(df.groupBy("qa_Country").count().distinct().orderBy("qa_Country", ascending=False).show()) 


# Questions from Online Retail

# Questions 1

# def question_1_price_giftcards_duvida (df):
# 	question_5_qa_UnitPrice(df)
# 	print(df.where(F.col("StockCode").startswith("gift_0001"))
# 	.agg(F.sum(F.col("UnitPrice") * F.col("Quantity")).alias("Gift Cards")).show())

def question_1_price_giftcards(df):
	df = df.withColumn('UnitPrice', F.regexp_replace(F.col('UnitPrice'), ',', '.').cast('float'))
	print(df.where(F.col('StockCode').rlike('gift_0001'))
	.agg(F.round((F.sum(F.col('UnitPrice') * F.col('Quantity'))),2).alias('Total_Gift_Cards')).show())

# Questions 2

def question_2_price_giftcards_sold_month (df):
	df = (df.withColumn('UnitPrice', F.regexp_replace(F.col('UnitPrice'), ',', '.').cast('float'))
	.withColumn("InvoiceDate", F.to_timestamp(F.col("InvoiceDate"), "d/M/yyyy H:m")))
	print(df.where(F.col('StockCode').rlike('gift_0001'))
	.groupBy('StockCode', F.month("InvoiceDate").alias('mes'))
	.agg(F.round(F.sum('UnitPrice'), 2).alias('Total_Gift_Cards_Mes'))
	.orderBy('Total_Gift_Cards_Mes', ascending=False).show())

# Test Function

def testing (df):
	pass


#End function to perform other functions

def Final_Boss_QA (df):
	question_1_qa_InvoiceNo(df)
	question_2_qa_StockCode(df)
	question_3_qa_Description(df)
	question_4_qa_InvoiceDate(df)
	question_5_qa_UnitPrice(df)
	question_6_qa_CustomerID(df)
	question_7_qa_Country(df)
	question_1_price_giftcards(df)

if __name__ == "__main__":
	sc = SparkContext()
	spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Online Retail]"))

	df = (spark.getOrCreate().read
		          .format("csv")
		          .option("header", "true")
		          .schema(schema_online_retail)
		          .load("/home/spark/capgemini-aceleracao-pyspark/data/online-retail/online-retail.csv"))
	#print(df.printSchema())
	#question_1_qa_InvoiceNo(df)
	#pergunta_1_qa(df)
	#print(df_final.show(999999))
	#Final_Boss (df)
	#testing(df)

	#question_5_qa_UnitPrice(df)
	#print(df.show())
	#print(df.printSchema)

	question_2_price_giftcards_sold_month(df)