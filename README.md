# Data Warehouse for Ultrarunning Analysis

This project is a Scala-based Apache Spark application that processes and analyzes ultrarunning race data. It transforms the data into a star schema and generates CSV reports for advanced analytics and visualization using Power BI.

## Features
- **Data Warehouse Design**: Implements a star schema for efficient querying of race and runner data.
- **Apache Spark Integration**: Processes large datasets and generates structured outputs.
- **Statistical Reports**:
  - Races by country and year.
  - Average finish times by age category for 50km races.
  - Participation trends for Greek runners by year.
  - Fastest races for each distance based on average speed.
  - Participation cube by country, race distance, and runner gender.
- **Power BI Compatibility**: Exports CSV files ready for import and visualization.

## Prerequisites
- Ubuntu 22.04.5
- Scala 2.12
- Apache Spark
- SBT
- Power BI (optional, for visualization)

## Preparing the Data
Before running the project, extract the `raceData.txt` file from the provided `raceData.zip`:

```bash
unzip raceData.zip
```
## Compile:
```
sbt package
```
##Run:
```
/opt/spark/bin/spark-submit --class "project" --master local[*] target/scala-2.12/myproject_2.12-0.1.0.jar
>>>>>>> c3a2c4284b1fa6272f7ecf36b84f4d755a179eaf
```
