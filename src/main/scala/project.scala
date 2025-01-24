//CHRISTOS BAMPOULIS
//p3312411
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession

object project {
    def main(args:Array[String])={
        val spark = SparkSession.builder.appName("My Project").master("local[*]").getOrCreate()
        import spark.implicits._
  
        spark.sparkContext.setLogLevel("WARN")
        val sc = spark.sparkContext
        // READ FILE
        val raceDataInput = sc.textFile("raceData.txt")
        // SCHEMA CREATION
        val raceSchema = StructType(Array(
            StructField("raceID", IntegerType, true),
            StructField("raceDate", StringType, true),
            StructField("raceName", StringType, true),
            StructField("raceDistance", StringType, true),
            StructField("raceCountry", StringType, true),
            StructField("runnerID", IntegerType, true),
            StructField("runnerBirthYear", IntegerType, true),
            StructField("runnerGender", StringType, true),
            StructField("runnerCountry", StringType, true),
            StructField("ageCategoryCode", StringType, true),
            StructField("ageCategoryTitle", StringType, true),
            StructField("performance", StringType, true),
            StructField("finishTime", StringType, true),
            StructField("averageSpeed", DoubleType, true)
            ))
        
        // FILTER HEADER OUT
        val header = raceDataInput.first()
        // DATA FILL
        val raceDataRows = raceDataInput.filter(line => line != header).map(line => line.split("\\|")).map(columns => Row(
        columns(0).toInt,
        columns(1),
        columns(2),
        columns(3),
        columns(4),
        columns(5).toInt,
        columns(6).toInt,
        columns(7),
        columns(8),
        columns(9),
        columns(10),
        columns(11),
        columns(12),
        columns(13).toDouble
        ))
        // DATA FRAME CREATION
        val raceDataFrame = spark.createDataFrame(raceDataRows, raceSchema)

        // CREATION OF DATA TABLE
        raceDataFrame.createOrReplaceTempView("raceDataFrameTable")
        // RUNNER TABLE CREATION
        val runnerTable = spark.sql("""
                                    SELECT DISTINCT runnerID, runnerBirthYear, runnerGender, runnerCountry 
                                    FROM raceDataFrameTable
                                    """)
        runnerTable.createOrReplaceTempView("runnerTable")
        runnerTable.show()
        // RACE TABLE CREATION
        val raceTable = spark.sql("""
                                    SELECT DISTINCT raceID, raceCountry, raceDistance, LOWER(raceName) AS raceName 
                                    FROM raceDataFrameTable
                                """)
        raceTable.createOrReplaceTempView("raceTable")
        raceTable.show()
        // TIME TABLE CREATION
        val timeTable = spark.sql(""" 
                                    SELECT DISTINCT raceDate, YEAR(TO_DATE(raceDate, 'yyyy-MM-dd')) AS year, MONTH(TO_DATE(raceDate, 'yyyy-MM-dd')) AS month, DAY(TO_DATE(raceDate, 'yyyy-MM-dd')) AS day 
                                    FROM raceDataFrameTable 
                                """)
        timeTable.createOrReplaceTempView("timeTable")
        timeTable.show()
        // AGES TABLE CREATION
        val ageTable = spark.sql("""
                                    SELECT DISTINCT ageCategoryCode, ageCategoryTitle 
                                    FROM raceDataFrameTable
                                """)
        ageTable.createOrReplaceTempView("ageTable")
        ageTable.show()
        // FACT TABLE CREATION
        val factTable = spark.sql("""
                                    SELECT raceID, runnerID, raceDate, ageCategoryCode, finishTime, performance, averageSpeed, raceDistance 
                                    FROM raceDataFrameTable
                                    """)
        factTable.createOrReplaceTempView("factTable")
        factTable.show()

        //1st REPORT Creation
        val racesPerCountryYear = spark.sql(""" 
                                            SELECT raceTable.raceCountry AS Country, timeTable.year AS Year, COUNT(factTable.raceID) AS `Number of Races` 
                                            FROM factTable 
                                            JOIN raceTable ON factTable.raceID = raceTable.raceID 
                                            JOIN timeTable ON factTable.raceDate = timeTable.raceDate 
                                            GROUP BY raceTable.raceCountry, timeTable.year 
                                            ORDER BY raceTable.raceCountry ASC, timeTable.year ASC 
                                            """)
        racesPerCountryYear.createOrReplaceTempView("racesPerCountryYear")
        racesPerCountryYear.coalesce(1).write.mode("overwrite").option("header", "true").csv("reports/racesPerCountryYear.csv")
        racesPerCountryYear.show()

        //2nd REPORT Creation
        val finishTimePerAge = spark.sql(""" 
                                        SELECT ageTable.ageCategoryTitle AS `Age Category`, AVG(factTable.finishTime) AS `Average Finish Time` 
                                        FROM factTable 
                                        JOIN ageTable ON factTable.ageCategoryCode = ageTable.ageCategoryCode 
                                        JOIN raceTable ON factTable.raceID = raceTable.raceID 
                                        WHERE raceTable.raceDistance = '50km' 
                                        GROUP BY ageTable.ageCategoryTitle 
                                        ORDER BY ageCategoryTitle ASC 
                                        """)
        finishTimePerAge.createOrReplaceTempView("finishTimePerAge")
        finishTimePerAge.coalesce(1).write.mode("overwrite").option("header", "true").csv("reports/finishTimePerAge.csv")
        finishTimePerAge.show()

        //3rd REPORT Creation
        val greekRunnersPerYear = spark.sql(""" 
                                            SELECT timeTable.year AS Year, COUNT(DISTINCT factTable.runnerID) AS `Number of Greek Runners` 
                                            FROM factTable JOIN runnerTable ON factTable.runnerID = runnerTable.runnerID JOIN timeTable ON factTable.raceDate = timeTable.raceDate 
                                            WHERE runnerTable.runnerCountry = 'GRE' 
                                            GROUP BY timeTable.year 
                                            ORDER BY timeTable.year ASC 
                                            """)
        greekRunnersPerYear.createOrReplaceTempView("greekRunnersPerYear")
        greekRunnersPerYear.coalesce(1).write.mode("overwrite").option("header", "true").csv("reports/greekRunnersPerYear.csv")
        greekRunnersPerYear.show()
        
        //4th REPORT Creation
        val avgSpeedPerRace = spark.sql("""  SELECT raceTable.raceDistance AS `Race Distance`, raceTable.raceName AS `Race's Name`, AVG(factTable.averageSpeed) AS `Average Runners' Performance`
                                    FROM factTable 
                                    JOIN raceTable ON factTable.raceID = raceTable.raceID
                                    GROUP BY raceTable.raceDistance, raceTable.raceName
                                """)
        avgSpeedPerRace.createOrReplaceTempView("avgSpeedPerRace")
        val maxRacePerDistance = spark.sql("""  
                                            SELECT outerQuery.`Race Distance`, outerQuery.`Race's Name`, outerQuery.`Average Runners' Performance` 
                                            FROM avgSpeedPerRace outerQuery
                                            WHERE 
                                            outerQuery.`Average Runners' Performance` = (
                                                SELECT 
                                                    MAX(innerQuery.`Average Runners' Performance`)
                                                FROM 
                                                    avgSpeedPerRace innerQuery
                                                WHERE 
                                                    innerQuery.`Race Distance` = outerQuery.`Race Distance`
                                            )
                                            ORDER BY 
                                            outerQuery.`Race Distance` DESC
                                            """)
        maxRacePerDistance.createOrReplaceTempView("maxRacePerDistance")
        maxRacePerDistance.coalesce(1).write.mode("overwrite").option("header", "true").csv("reports/maxRacePerDistance.csv")
        maxRacePerDistance.show()

        //5th REPORT Creation
        val participationsCube = spark.sql("""   
                                            SELECT 
                                                raceTable.raceCountry AS `Race's Country`, 
                                                raceTable.raceDistance AS `Race's Distance`, 
                                                runnerTable.runnerGender AS `Runner's Gender`, 
                                                COUNT(factTable.runnerID) AS `Participation Count`
                                            FROM factTable
                                            JOIN raceTable ON factTable.raceID = raceTable.raceID
                                            JOIN runnerTable ON factTable.runnerID = runnerTable.runnerID
                                            GROUP BY CUBE(raceTable.raceCountry, raceTable.raceDistance, runnerTable.runnerGender)
                                            ORDER BY `Race's Country` ASC, `Race's Distance` ASC, `Runner's Gender` ASC
                                            """)
        participationsCube.createOrReplaceTempView("participationsCube")
        participationsCube.coalesce(1).write.mode("overwrite").option("header", "true").csv("reports/participationsCube.csv")
        participationsCube.show()

        //WRITE STAR SCHEMA TO CSVs
        factTable.coalesce(1).write.mode("overwrite").option("header", "true").csv("tables/factTable.csv")
        raceTable.coalesce(1).write.mode("overwrite").option("header", "true").csv("tables/raceTable.csv")
        runnerTable.coalesce(1).write.mode("overwrite").option("header", "true").csv("tables/runnerTable.csv")
        ageTable.coalesce(1).write.mode("overwrite").option("header", "true").csv("tables/ageTable.csv")
        timeTable.coalesce(1).write.mode("overwrite").option("header", "true").csv("tables/timeTable.csv")

    }
}


