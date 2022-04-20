from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

schema_census_income = StructType([
	StructField("age",  IntegerType(),  True),
	StructField("workclass",  StringType(),  True),
	StructField("fnlwgt",  IntegerType(),  True),
	StructField("education",  StringType(),  True),
	StructField("education-num",  IntegerType(),  True),
	StructField("marital-status",  StringType(),  True),
	StructField("occupation",  StringType(),  True),
	StructField("relashionship",  StringType(),  True),
	StructField("race",  StringType(),  True),
	StructField("sex",  StringType(),  True),
	StructField("capital-gain",  IntegerType(),  True),
	StructField("capital-loss",  IntegerType(),  True),
	StructField("hours-per-week",  IntegerType(),  True),
	StructField("native-country",  StringType(),  True),
	StructField("income",  StringType(),  True),
])

		
def census_income_tr(df):
	#workclass
	df = (df.withColumn('workclass', 
	F.when(F.col('workclass').rlike('\?'), None)
	.otherwise(F.col('workclass')))
	)

	#occupation
	df = (df.withColumn('occupation', 
	F.when(F.col('occupation').rlike('\?'), None)
	.otherwise(F.col('occupation')))
	)

	# native-country
	df = (df.withColumn('native-country', 
	F.when(F.col('native-country').rlike('\?'), None)
	.otherwise(F.col('native-country')))
	)
	return df

def question_1(df):
	(df.where(F.col('workclass').isNotNull() & F.col('income').rlike('\>50K'))
	.groupBy(F.col('workclass'), F.col('income'))
	.count()
	.orderBy(F.col('count').desc())
	.limit(1)
	.show()
	)

def question_2(df):
	# coluna a se usar race,hours-per-week
	(df.groupBy("race")
	.agg(
		F.avg(
			F.col("hours-per-week")
		).alias("avg_horas")
	).show())

def question_3_and_4(df):
	(df.groupBy('sex')
	.agg(F.count(F.col('sex')).alias('total'))
	.withColumn('proporcao', F.round((F.col('total')/df.count()), 2))
	.show()
	)

def question_5(df):
	(df.groupBy("occupation")
	.agg(
		F.avg(
			F.col("hours-per-week"))
	.alias("Total_horas"))
	.orderBy(F.col("Total_horas")
	.desc())
	.limit(1)
	.show())

def question_6(df):
	(df.groupBy("occupation","education")
	.count()
	.orderBy(F.col("count")
	.desc())
	.limit(1)
	.show())

def question_7(df):
	(df.groupBy("sex","occupation")
	.count()
	.orderBy(F.col("count")
	.desc())
	.limit(1)
	.show())

def question_8(df):
	(df.where(F.col("education").contains("Doctorate"))
	.groupBy("education","race")
	.count()
	.orderBy(F.col("count")
	.desc())
	.show())

def question_9(df):
	(df.where(F.col("workclass").contains("Self-emp"))
	.groupBy("education","race","sex")
	.count()
	.orderBy(F.col("count")
	.desc())
	.limit(1)
	.show())

if __name__ == "__main__":
	sc = SparkContext()
	spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Census Income]"))

	df = (spark.getOrCreate().read
		          .format("csv")
		          .option("header", "true")
		          .schema(schema_census_income)
		          .load("/home/spark/capgemini-aceleracao-pyspark/data/census-income/census-income.csv"))

	df_tr = census_income_tr(df)
	# df_tr = qa_workclass(df)
	question_8(df_tr)