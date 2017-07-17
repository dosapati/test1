import org.apache.spark.sql.Row;

val c2 = sc.textFile("/user/hive/warehouse/test1/xaa")

var rowRDD = c2.map(_.split(",")).map(p => Row(p(0),p(1),p(2),p(3),p(4),p(5),p(6),p(7),p(8),p(9),p(10),p(11),p(12),p(13),p(14),p(1),p(15),p(16),p(17),p(18),p(19),p(20),p(21),p(22),p(23),p(24),p(25),p(26),p(27),p(28)))
val schemaStr = "DESYNPUF_ID	BENE_BIRTH_DT	BENE_DEATH_DT	BENE_SEX_IDENT_CD	BENE_RACE_CD"
val schema =StructType(schemaStr.split(" ").map(fieldName => StructField(fieldName, LongType, true)))
import org.apache.spark.sql.types.{StructType,StructField,StringType,LongType,IntegerType};
val schema =
   StructType(
     StructField("DESYNPUF_ID", StringType, false) ::
     StructField("BENE_BIRTH_DT", IntegerType, false) ::
     StructField("BENE_SEX_IDENT_CD", IntegerType, false) ::
     StructField("BENE_RACE_CD", IntegerType, false) ::
     StructField("BENE_ESRD_IND", StringType, false) :: Nil)
val c2 = sc.textFile("/user/hive/warehouse/test1/c2.csv")

var rowRDD = c2.map(_.split(",")).map(p => Row(p(0),(p(1).toInt),p(2).toInt,p(3).toInt,p(4)))

val cDF = sqlContext.createDataFrame(rowRDD, schema)

cDF.registerTempTable("c01")

val results = sqlContext.sql("SELECT * FROM c01")

val results = sqlContext.sql("SELECT * FROM c01 where BENE_BIRTH_DT > 1930000")


results.count
