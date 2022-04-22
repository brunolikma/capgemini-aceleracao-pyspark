from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
 
schema_communities_crime = StructType([ 
    StructField('state', IntegerType(), True),
    StructField('county', IntegerType(), True),
    StructField('community', IntegerType(), True),
    StructField('communityname', StringType(), True),
    StructField('fold', IntegerType(), True),
    StructField('population', FloatType(), True),
    StructField('householdsize', FloatType(), True),
    StructField('racepctblack', FloatType(),  True),
    StructField('racePctWhite',FloatType(), True),
    StructField('racePctAsian', FloatType(), True),
    StructField('racePctHisp', FloatType(), True),
    StructField('agePct12t21', FloatType(), True),
    StructField('agePct12t29', FloatType(), True),
    StructField('agePct16t24', FloatType(), True),
    StructField('agePct65up', FloatType(), True),
    StructField('numbUrban', FloatType(), True),
    StructField('pctUrban', FloatType(), True),
    StructField('medIncome', FloatType(), True),
    StructField('pctWWage', FloatType(), True),
    StructField('pctWFarmSelf', FloatType(), True),
    StructField('pctWInvInc', FloatType(), True),
    StructField('pctWSocSec', FloatType(), True),
    StructField('pctWPubAsst', FloatType(), True),
    StructField('pctWRetire', FloatType(), True),
    StructField('medFamInc', FloatType(), True),
    StructField('perCapInc', FloatType(), True),
    StructField('whitePerCap', FloatType(), True),
    StructField('blackPerCap', FloatType(), True),
    StructField('indianPerCap', FloatType(), True),
    StructField('AsianPerCap', FloatType(), True),
    StructField('OtherPerCap', FloatType(), True),
    StructField('HispPerCap', FloatType(), True),
    StructField('NumUnderPov', FloatType(), True),
    StructField('PctPopUnderPov', FloatType(), True),
    StructField('PctLess9thGrade', FloatType(), True),
    StructField('PctNotHSGrad', FloatType(), True),
    StructField('PctBSorMore', FloatType(), True),
    StructField('PctUnemployed', FloatType(), True),
    StructField('PctEmploy', FloatType(), True),
    StructField('PctEmplManu', FloatType(), True),
    StructField('PctEmplProfServ', FloatType(), True),
    StructField('PctOccupManu', FloatType(), True),
    StructField('PctOccupMgmtProf', FloatType(), True),
    StructField('MalePctDivorce', FloatType(), True),
    StructField('MalePctNevMarr', FloatType(), True),
    StructField('FemalePctDiv', FloatType(), True),
    StructField('TotalPctDiv', FloatType(), True),
    StructField('PersPerFam', FloatType(), True),
    StructField('PctFam2Par', FloatType(), True),
    StructField('PctKids2Par', FloatType(), True),
    StructField('PctYoungKids2Par', FloatType(), True),
    StructField('PctTeen2Par', FloatType(), True),
    StructField('PctWorkMomYoungKids', FloatType(), True),
    StructField('PctWorkMom', FloatType(), True),
    StructField('NumIlleg', FloatType(), True),
    StructField('PctIlleg', FloatType(), True),
    StructField('NumImmig', FloatType(), True),
    StructField('PctImmigRecent', FloatType(), True),
    StructField('PctImmigRec5', FloatType(), True),
    StructField('PctImmigRec8', FloatType(), True),
    StructField('PctImmigRec10', FloatType(), True),
    StructField('PctRecentImmig', FloatType(), True),
    StructField('PctRecImmig5', FloatType(), True),
    StructField('PctRecImmig8', FloatType(), True),
    StructField('PctRecImmig10', FloatType(), True),
    StructField('PctSpeakEnglOnly', FloatType(), True),
    StructField('PctNotSpeakEnglWell', FloatType(), True),
    StructField('PctLargHouseFam', FloatType(), True),
    StructField('PctLargHouseOccup', FloatType(), True),
    StructField('PersPerOccupHous', FloatType(), True),
    StructField('PersPerOwnOccHous', FloatType(), True),
    StructField('PersPerRentOccHous', FloatType(), True),
    StructField('PctPersOwnOccup', FloatType(), True),
    StructField('PctPersDenseHous', FloatType(), True),
    StructField('PctHousLess3BR', FloatType(), True),
    StructField('MedNumBR', FloatType(), True),
    StructField('HousVacant', FloatType(), True),
    StructField('PctHousOccup', FloatType(), True),
    StructField('PctHousOwnOcc', FloatType(), True),
    StructField('PctVacantBoarded', FloatType(), True),
    StructField('PctVacMore6Mos', FloatType(), True),
    StructField('MedYrHousBuilt', FloatType(), True),
    StructField('PctHousNoPhone', FloatType(), True),
    StructField('PctWOFullPlumb', FloatType(), True),
    StructField('OwnOccLowQuart', FloatType(), True),
    StructField('OwnOccMedVal', FloatType(), True),
    StructField('OwnOccHiQuart', FloatType(), True),
    StructField('RentLowQ', FloatType(), True),
    StructField('RentMedian', FloatType(), True),
    StructField('RentHighQ', FloatType(), True),
    StructField('MedRent', FloatType(), True),
    StructField('MedRentPctHousInc', FloatType(), True),
    StructField('MedOwnCostPctInc', FloatType(), True),
    StructField('MedOwnCostPctIncNoMtg', FloatType(), True),
    StructField('NumInShelters', FloatType(), True),
    StructField('NumStreet', FloatType(), True),
    StructField('PctForeignBorn', FloatType(), True),
    StructField('PctBornSameState', FloatType(), True),
    StructField('PctSameHouse85', FloatType(), True),
    StructField('PctSameCity85', FloatType(), True),
    StructField('PctSameState85', FloatType(), True),
    StructField('LemasSwornFT', FloatType(), True),
    StructField('LemasSwFTPerPop', FloatType(), True),
    StructField('LemasSwFTFieldOps', FloatType(), True),
    StructField('LemasSwFTFieldPerPop', FloatType(), True),
    StructField('LemasTotalReq', FloatType(), True),
    StructField('LemasTotReqPerPop', FloatType(), True),
    StructField('PolicReqPerOffic', FloatType(), True),
    StructField('PolicPerPop', FloatType(), True),
    StructField('RacialMatchCommPol', FloatType(), True),
    StructField('PctPolicWhite', FloatType(), True),
    StructField('PctPolicBlack', FloatType(), True),
    StructField('PctPolicHisp', FloatType(), True),
    StructField('PctPolicAsian', FloatType(), True),
    StructField('PctPolicMinor', FloatType(), True),
    StructField('OfficAssgnDrugUnits', FloatType(), True),
    StructField('NumKindsDrugsSeiz', FloatType(), True),
    StructField('PolicAveOTWorked', FloatType(), True),
    StructField('LandArea', FloatType(), True),
    StructField('PopDens', FloatType(), True),
    StructField('PctUsePubTrans', FloatType(), True),
    StructField('PolicCars', FloatType(), True),
    StructField('PolicOperBudg', FloatType(), True),
    StructField('LemasPctPolicOnPatr', FloatType(), True),
    StructField('LemasGangUnitDeploy', FloatType(), True),
    StructField('LemasPctOfficDrugUn', FloatType(), True),
    StructField('PolicBudgPerPop', FloatType(), True),
    StructField('ViolentCrimesPerPop', FloatType(), True),
     ])

if __name__ == "__main__":
	sc = SparkContext()
	spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Communities & Crime]"))

	df = (spark.getOrCreate().read
		          .format("csv")
		          .option("header", "true")
		          .schema(schema_communities_crime)
		          .load("/home/spark/capgemini-aceleracao-pyspark/data/communities-crime/communities-crime.csv"))
	

def create_column(df):
	df = (df.withColumn("HighestRace", 
	F.when((F.col("racepctblack") > F.col("racePctWhite")) &
		   (F.col("racepctblack") > F.col("racePctAsian")) &
		   (F.col("racepctblack") > F.col("racePctHisp")), "african american")
	.when((F.col("racePctWhite") > F.col("racePctAsian")) &
		  (F.col("racePctWhite") > F.col("racePctHisp")),"caucasian ")
	.when((F.col("racePctAsian") > F.col("racePctHisp")), "asian heritage")
	.otherwise("hispanic heritage")))

def isNa(df): #limpar null
	names =df.schema.names
	for c in names:
		df=df.withColumn(c, (
			F.when(
				F.col(c).contains("?"), None
			).otherwise(
				F.col(c))
			)
		)
		return df

df = isNa(df)


def codigo_teste(df):
	(df.select("ViolentCrimesPerPop").show())
	print(df.printSchema)

def question_1(df):
	(df.groupBy('state','communityname')
	.agg(
		F.round(
			F.sum(
				F.col("PolicOperBudg")),2)
	.alias("MaxSumPolicOperBudg"))
	.orderBy(F.col("MaxSumPolicOperBudg")
	.show(1))

def question_2(df):
	(df.groupBy("state","communityname")
	.agg(
		F.round(
			F.sum(
				F.col("ViolentCrimesPerPop")),2)
	.alias("HighestNumberOfViolentCrimes"))
	.orderBy(F.col("HighestNumberOfViolentCrimes")
	.show(1))

def question_3(df):
	(df.groupBy("state","communityname")
	.agg(
		F.round(
			F.sum(
				F.col("population")),2)
	.alias("HighestPopulation"))
	.orderBy(F.col("HighestPopulation")
	.show(1))

def question_4(df):
	(df.groupBy("state","communityname")
	.agg(
		F.round(
			F.sum(
				F.col("racepctblack")),2)
	.alias("CommunityHasTheLargestBlackPopulation"))
	.orderBy(F.col("CommunityHasTheLargestBlackPopulation")
	.show(1))

def question_5(df):
	(df.groupBy("state","communityname")
	.agg(
		F.round(
			F.sum(
				F.col("pctWWage")),2)
	.alias("WhichCommunityHasTheHighestPercentageOfPeopleReceivingSalary"))
	.orderBy(F.col("WhichCommunityHasTheHighestPercentageOfPeopleReceivingSalary")
	.show(1))

def question_6(df):
	(df.groupBy("state","communityname")
	.agg(
		F.round(
			F.sum(
				F.col("agePct12t21")),2)
	.alias("WhichCommunityHasTheLargestYouthPopulation"))
	.orderBy(F.col("WhichCommunityHasTheLargestYouthPopulation")
	.show(1))

def question_7(df):
	(df.agg(
		F.round(
			F.corr("PolicOperBudg","ViolentCrimesPerPop"),2)
	.alias("CorrPolicOperBudgAndViolentCrimesPerPop"))
	.show())

def question_8(df):
	(df.agg(
		F.round(
			F.corr("PctPolicWhite","PolicOperBudg"),2)
	.alias("CorrPctPolicWhiteAndPolicOperBudg"))
	.show())

def question_9(df):
	(df.agg(
		F.round(
			F.corr("population","PolicOperBudg"),2)
	.alias("CorrPopulationAndPolicOperBudg"))
	.show())

def question_10(df):
	(df.agg(
		F.round(
			F.corr("population","ViolentCrimesPerPop"),2)
	.alias("CorrPopulationAndViolentCrimesPerPop"))
	.show())

def question_11(df):
	(df.agg(
		F.round(
			F.corr("medIncome","ViolentCrimesPerPop"),2)
	.alias("CorrMedIncomeAndViolentCrimesPerPop"))
	.show())

def question_12(df):
	df = (df.withColumn("HighestRace", 
	F.when((F.col("racepctblack") > F.col("racePctWhite")) &
		   (F.col("racepctblack") > F.col("racePctAsian")) &
		   (F.col("racepctblack") > F.col("racePctHisp")), "african american")
	.when((F.col("racePctWhite") > F.col("racePctAsian")) &
		  (F.col("racePctWhite") > F.col("racePctHisp")),"caucasian")
	.when((F.col("racePctAsian") > F.col("racePctHisp")), "asian heritage")
	.otherwise("hispanic heritage")))
	(df.groupBy("state","communityname","HighestRace")
	   .agg(
			F.round(
				F.sum(
					F.col("ViolentCrimesPerPop")),2)
		.alias("HighestNumberOfViolentCrimes"))
		.orderBy(F.col("HighestNumberOfViolentCrimes")
		.desc())
		.limit(10)
		.show())

def enforcement_function_for_communities_and_crimes(df):
	print("Question 1")
	question_1(df)
	print("Question 2")
	question_2(df)
	print("Question 3")
	question_3(df)
	print("Question 4")
	question_4(df)
	print("Question 5")
	question_5(df)
	print("Question 6")
	question_6 df)
	print("Question 7")
	question_7(df)
	print("Question 8")
	question_8(df)
	print("Question 9")
	question_9(df)
	print("Question 10")
	question_10(df)
	print("Question 11")
	question_11(df)
	print("Question 12")
	question_12(df)



	enforcement_function_for_communities_and_crimes(df)
