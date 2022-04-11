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
	StructField("UnitPrice",  FloatType(),  True),
	StructField("CustomerID",  IntegerType(),  True),
	StructField("Country",  StringType(),  True),
])

def question_1_qa_InvoiceNo (df):
	df = df.withColumn('qa_InvoiceNo', (
		F.when(F.col("InvoiceNo").startswith("C"), "Cancelled")
		.when((F.col("InvoiceNo").rlike("^[0-9]*$")), "Effective")
		.otherwise("Unknown")
	))
	print(df.groupBy("qa_InvoiceNo").count().show())

def question_2_qa_StockCode (df):
	df = df.withColumn("qa_StockCode", (
		F.when(~F.col("StockCode").rlike("([0-9a-zA-Z]{5})"), "Unknown")
		.otherwise("Effective")
	))
	print(df.groupBy("qa_StockCode").count().show())

def question_2_qa_StockCode (df):
	

def testing (df):
	pass

def Final_Boss (df):
	question_1_qa_InvoiceNo(df)
	question_2_qa_StockCode(df)


if __name__ == "__main__":
	sc = SparkContext()
	spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Online Retail]"))

	df = (spark.getOrCreate().read
		          .format("csv")
		          .option("header", "true")
		          .schema(schema_online_retail)
		          .load("/home/spark/capgemini-aceleracao-pyspark/data/online-retail/online-retail.csv"))
	#print(df.printSchema())
	#testCoding(df)
	#question_1_qa_InvoiceNo(df)
	#pergunta_1_qa(df)
	#print(df_final.show(999999))
	Final_Boss (df)