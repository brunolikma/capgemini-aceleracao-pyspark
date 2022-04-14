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
	return df

# Questions QA 2

def question_2_qa_StockCode (df):
	df = df.withColumn("qa_StockCode", (
		F.when(~F.col("StockCode").rlike("([0-9a-zA-Z]{5})"), "Missing")
		.otherwise("Effective")
	))
	return df
# Questions QA 3

def question_3_qa_Description (df):
	df = df.withColumn("qa_Description", (
		F.when(F.col("Description").isNull(), "Missing")
		.when(F.col("Description") == "", "Missing")
		.otherwise("Effective")
		))
	return df

# Questions QA 4

def question_4_qa_InvoiceDate (df):
	df = df.withColumn("InvoiceDate",(F.to_timestamp(F.col("InvoiceDate"), "d/M/yyyy H:m")))
	return df

# Questions QA 5

def question_5_qa_UnitPrice (df):
	return df.withColumn('UnitPrice', F.regexp_replace(F.col('UnitPrice'), ',', '.').cast('float'))
	

# Questions QA 6

def question_6_qa_CustomerID(df):
	df = df.withColumn("qa_CustomerID", (
	F.when(~F.col("CustomerID").rlike("([0-9a-zA-Z]{5})"), "Failure").otherwise("Effective")))
	return df

# Questions QA 7

def question_7_qa_Country(df):
	df = df.withColumn("qa_Country", (
	F.when(F.col("Country").isNull(), "Missing")
	 .when(F.col("Country") == "", "Missing")
	 .otherwise("Effective")))
	return df

def standard_treatment(df):
	df = (df.withColumn("Quantity", F.when(F.col("Quantity").isNull() | (F.col("Quantity") < 0), 0)
            .otherwise(F.col("Quantity")))
            .withColumn("InvoiceDate", F.to_timestamp(F.col("InvoiceDate"), "d/M/yyyy H:m"))
            .withColumn('UnitPrice', F.regexp_replace(F.col('UnitPrice'), ',', '.').cast('float'))
            .withColumn('UnitPrice', F.when(F.col('UnitPrice').isNull() | (F.col('UnitPrice') < 0), 0)
            .otherwise(F.col('UnitPrice'))))
	return df		
	
# Questions from Online Retail

# Questions 1

def question_1_price_giftcards(df):
	(question_5_qa_UnitPrice(df).where(F.col('StockCode')
								.rlike('gift_0001'))
	                            .agg(F.round((F.sum(F.col('UnitPrice') * F.col('Quantity'))), 2)
								.alias('Total_Gift_Cards'))
								.show())		
	

# Questions 2

def question_2_price_giftcards_sold_month (df):
	(question_4_qa_InvoiceDate(df).where(F.col('StockCode')
								  .rlike('gift_0001')
								  .alias('Gift_Cards'))
								  .groupBy(F.month("InvoiceDate")
								  .alias('mes'))
							      .agg(F.round(F.sum('UnitPrice'), 2)
								  .alias('Giftcards_Sold_Month'))
								  .orderBy('Total_Gift_Cards_Mes')
								  .show())

# Questions 3

def question_3_sample(df):
	df = (df.withColumn('UnitPrice', F.regexp_replace(F.col('UnitPrice'), ',', '.').cast('float'))
	.withColumn("UnitPrice", F.when(F.col("InvoiceNo").startswith('C'), 0).otherwise(F.col("UnitPrice"))))
	print(df.where(F.col('StockCode')== 'S')
	.agg(F.round(F.sum(F.col('UnitPrice')), 2).alias('Total_Amostras_Concedidas')).show())

# Questions 4

def question_4_best_selling_item(df):
	df = (df.withColumn("Quantity", F.when(F.col("Quantity").isNull(), 0)
	.when(F.col("Quantity") < 0, 0).otherwise(F.col("Quantity"))))
	print(df.where(~F.col('InvoiceNo').rlike('C'))
	.groupBy(F.col('Description'))
	.agg(F.sum('Quantity').alias('Quantity'))
	.orderBy(F.col('Quantity').desc())
	.limit(1)
	.show())

# Questions 5

def question_5_best_selling_item_for_month(df):
    df = (standard_treatment(df).where(~F.col('InvoiceNo').rlike('C'))
            .groupBy('Description', F.month('InvoiceDate')
			.alias('month'))
            .agg(F.sum('Quantity')
			.alias('Quantity'))
            .orderBy(F.col('Quantity').desc())
			.dropDuplicates(['month'])
            .show())

# Questions 6

def question_6_best_time_of_day_has_higher_sales_value(df):
    df = (standard_treatment(df).where(~F.col('InvoiceNo').rlike('C'))
            .groupBy(F.hour('InvoiceDate').alias('hora_de_maior_venda'))
            .agg(F.round(F.sum(F.col('UnitPrice') * F.col('Quantity')), 2).alias('valor'))
            .orderBy(F.col('valor').desc())
            .limit(1)
            .show())

# Questions 7

def question_7_month_of_year_has_the_highest_sales_value(df):
    df = (standard_treatment(df).where(~F.col('InvoiceNo').rlike('C'))
            .groupBy(F.month('InvoiceDate').alias('mes_de_maior_venda'))
            .agg(F.round(F.sum(F.col('UnitPrice') * F.col('Quantity')), 2).alias('valor'))
            .orderBy(F.col('valor').desc())
            .limit(1)
            .show())

# Questions 8
def question_8_month_of_year_has_the_highest_sales_value(df):
		df = (standard_treatment(df).where(~F.col('InvoiceNo').rlike('C'))
            .groupBy('Description', F.year('InvoiceDate').alias('year'), F.month('InvoiceDate').alias('month'))
            .agg(F.round(F.sum(F.col('UnitPrice') * F.col('Quantity')), 2).alias('valor'))
            .orderBy(F.col('valor').desc()).dropDuplicates(['month'])
            .show())

# Questions 9

def question_9(df):
	df = (standard_treatment(df).where(~F.col('InvoiceNo').rlike('C'))
            .groupBy('Country')
            .agg(F.round(F.sum(F.col('UnitPrice') * F.col('Quantity')), 2).alias('valor'))
            .orderBy(F.col('valor').desc())
			.limit(1)
            .show())

# Questions 10

def question_10(df):
	df = (standard_treatment(df).where((F.col('StockCode')== 'M') & (~F.col('InvoiceNo').rlike('C')))
            .groupBy('Country')
            .agg(F.round(F.sum(F.col('UnitPrice') * F.col('Quantity')), 2).alias('valor'))
            .orderBy(F.col('valor').desc())
			.limit(1)
            .show())

def question_11(df):
	df = (standard_treatment(df).where(~F.col('InvoiceNo').rlike('C'))
			.groupBy('InvoiceNo')
            .agg(F.round(F.sum(F.col('UnitPrice') * F.col('Quantity')), 2).alias('valor'))
            .orderBy(F.col('valor').desc())
			.limit(1)
            .show())

def question_12(df):
	df = (standard_treatment(df).where(~F.col('InvoiceNo').rlike('C'))
			.groupBy('InvoiceNo')
            .agg(F.max("Quantity").alias('quantidade'))
            .orderBy(F.col('quantidade').desc())
			.limit(1)
            .show())

def question_13(df):
	(df.where(F.col("CustomerID").isNotNull())
	.groupBy(F.col('CustomerID').alias('customer'))
	.count()
	.orderBy(F.col('count').desc())
	.limit(1)
	.show())
	
# Test Function

def testing (df):
	pass


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
	# Final_Boss_QA(df)
	#testing(df)

	#question_5_qa_UnitPrice(df)
	#print(df.show())
	#print(df.printSchema)

	#question_1_price_giftcards_duvida_nao_funciona(df)
	question_4_qa_InvoiceDate(df)
	# question_2_price_giftcards_sold_month(df)