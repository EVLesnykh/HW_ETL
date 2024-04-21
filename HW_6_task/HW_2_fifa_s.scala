import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{col, collect_list, concat_ws}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
val t1 = System.currentTimeMillis()
val misqlcon = "jdbc:mysql://localhost:33061/spark?user=root&password=1"
val driver = "com.mysql.cj.jdbc.Driver" 
if(1==1){
var df1  = spark.read
        .option("header", "true")                                                
        .option("delimiter", ",")
				.option("encoding", "utf-8")                     
        .csv("/home/ekaterina/fifa_s_2.csv") 
		    
				df1=df1
     		.withColumn("ID",col("ID").cast("int")).dropDuplicates()
				.withColumn("Age",col("Age").cast("int"))
				.withColumn("Overall",col("Overall").cast("int"))
				.withColumn("Potential",col("Potential").cast("int"))        	
				.withColumn("Value",col("Value").cast("float")) 
				.withColumn("Wage",col("Wage").cast("float"))
				.withColumn("International Reputation",col("International Reputation").cast("float")) 
				.withColumn("Skill Moves",col("Skill Moves").cast("float"))
				.withColumn("Height",col("Height").cast("float")) 
				.withColumn("Weight",col("Weight").cast("float"))
				.withColumn("Release Clause",col("Release Clause").cast("float")) 

        .withColumn("Age_category",
				when(col("Age") <= 20, "before 20 years")
				.when(col("Age").between(20, 30), "20-30 years")
				.when(col("Age").between(30, 36), "30-36 years")
				.otherwise("after 36 years"))

		df1.write.format("jdbc")
		.option("url", misqlcon)
	  .option("driver", driver)
		.option("dbtable", "HW_2_task_3")
    .option("user", "root")
    .option("password", "1")
    .option("charset", "utf8mb4_general_ci")  
    .mode("overwrite")
    .save()
		
		df1.show()

val s = df1.columns.map(c => sum(col(c).isNull.cast("integer")).alias(c))
val df2 = df1.agg(s.head, s.tail:_*)
val t = df2.columns.map(c => df2.select(lit(c).alias("col_name"), col(c).alias("null_count")))
val df_agg_col = t.reduce((df1, df2) => df1.union(df2))
df_agg_col.show()
}
			
val s0 = (System.currentTimeMillis() - t1)/1000
val s = s0 % 60
val m = (s0/60) % 60
val h = (s0/60/60) % 24
println("%02d:%02d:%02d".format(h, m, s))


System.exit(0)