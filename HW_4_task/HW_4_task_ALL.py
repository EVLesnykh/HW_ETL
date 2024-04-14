import pyspark,time,platform,sys,os
from datetime import datetime
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col,lit,current_timestamp
import pandas as pd
import matplotlib.pyplot as plt 
from sqlalchemy import inspect,create_engine
from pandas.io import sql
import warnings,matplotlib
warnings.filterwarnings("ignore")
t0=time.time()
con=create_engine("mysql://root:24082019:jhf@localhost/spark")
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark=SparkSession.builder.appName("Hi").getOrCreate()

sql.execute("""drop table if exists spark.`HW_4_all`""",con)
sql.execute("""CREATE TABLE if not exists spark.`HW_4_all` (
	`№` INT(10) NULL DEFAULT NULL,
	`Month` DATE NULL DEFAULT NULL,
	`Payment amount` FLOAT NULL DEFAULT NULL,
	`Payment of the principal debt` FLOAT NULL DEFAULT NULL,
	`Payment of interest` FLOAT NULL DEFAULT NULL,
	`Balance of debt` FLOAT NULL DEFAULT NULL,
	`interest` FLOAT NULL DEFAULT NULL,
	`debt` FLOAT NULL DEFAULT NULL
)
COLLATE='utf8mb4_general_ci'
ENGINE=InnoDB""",con)
from pyspark.sql.window import Window
from pyspark.sql.functions import sum as sum1
w = Window.partitionBy(lit(1)).orderBy("№").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df1 = spark.read.format("com.crealytics.spark.excel")\
        .option("dataAddress", "'sheet_sem'!A1")\
        .option("useHeader", "false")\
        .option("treatEmptyValuesAsNulls", "false")\
        .option("inferSchema", "true").option("addColorColumns", "true")\
	.option("usePlainNumberFormat","true")\
        .option("startColumn", 0)\
        .option("endColumn", 99)\
        .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")\
        .option("maxRowsInMemory", 20)\
        .option("excerptSize", 10)\
        .option("header", "true")\
        .format("excel")\
        .load("/Users/Андрей/Desktop/git_education/HW_ETL/HW_4_task/s4_2_HW.xlsx").limit(1000)\
        .withColumn("interest", sum1(col("Payment of interest")).over(w))\
        .withColumn("debt", sum1(col("Payment of the principal debt")).over(w))

df2 = spark.read.format("com.crealytics.spark.excel")\
        .option("dataAddress", "'sheet_120'!A1:F135")\
        .option("useHeader", "false")\
        .option("treatEmptyValuesAsNulls", "false")\
        .option("inferSchema", "true").option("addColorColumns", "true")\
	.option("usePlainNumberFormat","true")\
        .option("startColumn", 0)\
        .option("endColumn", 99)\
        .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")\
        .option("maxRowsInMemory", 20)\
        .option("excerptSize", 10)\
        .option("header", "true")\
        .format("excel")\
        .load("/Users/Андрей/Desktop/git_education/HW_ETL/HW_4_task/s4_2_HW.xlsx").limit(1000)\
        .withColumn("interest", sum1(col("Payment of interest")).over(w))\
        .withColumn("debt", sum1(col("Payment of the principal debt")).over(w))


df3 = spark.read.format("com.crealytics.spark.excel")\
        .option("dataAddress", "'sheet_150'!A1:F93")\
        .option("useHeader", "false")\
        .option("treatEmptyValuesAsNulls", "false")\
        .option("inferSchema", "true").option("addColorColumns", "true")\
	.option("usePlainNumberFormat","true")\
        .option("startColumn", 0)\
        .option("endColumn", 99)\
        .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")\
        .option("maxRowsInMemory", 20)\
        .option("excerptSize", 10)\
        .option("header", "true")\
        .format("excel")\
        .load("/Users/Андрей/Desktop/git_education/HW_ETL/HW_4_task/s4_2_HW.xlsx").limit(1000)\
        .withColumn("interest", sum1(col("Payment of interest")).over(w))\
        .withColumn("debt", sum1(col("Payment of the principal debt")).over(w))

df_combined = df1.union(df2).union(df3)

df_combined.write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=24082019:jhf")\
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "HW_4_all")\
        .mode("append").save()

"""df_pandas = df_combined.toPandas()"""

df_pandas1 = df1.toPandas()
df_pandas2 = df2.toPandas()
df_pandas3 = df3.toPandas()

# Get current axis 
ax = plt.gca()
ax.ticklabel_format(style='plain')

# bar plot
df_pandas1.plot(kind='line', x='№', y='debt', color='green', ax=ax)
df_pandas1.plot(kind='line', x='№', y='interest', color='red', ax=ax)
df_pandas2.plot(kind='line', x='№', y='debt', color='grey', ax=ax) # ежемесячный платеж 120 000
df_pandas2.plot(kind='line', x='№', y='interest', color='orange', ax=ax)  # ежемесячный платеж 120 000
df_pandas3.plot(kind='line', x='№', y='debt', color='purple', ax=ax)  # ежемесячный платеж 150 000
df_pandas3.plot(kind='line', x='№', y='interest', color='yellow', ax=ax)  # ежемесячный платеж 150 000

# set the title 
plt.title('Loan Payments Over Tim (All graphics)')
plt.grid ( True )
ax.set(xlabel=None)

# show the plot 
plt.show() 
spark.stop()
t1=time.time()
print('finished',time.strftime('%H:%M:%S',time.gmtime(round(t1-t0))))
