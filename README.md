# boston_crimes_map_bordea

This program computes for each district in Boston following statistics: 
crimes_total (total amount of crimes), crimes_monthly (a median value of 
crimes per month), frequent_crime_types (3 most frequent types of crime), 
lat (average latitude of crimes) and lng (average longitude of crimes). 
As a template was used `MrPowers/spark-sbt.g8` project. You can get it with 
the following command:
```console
$ sbt new MrPowers/spark-sbt.g8
```

### How to install

First install [sbt](https://www.scala-sbt.org/download.html) and download 
[Spark](https://spark.apache.org/downloads.html) (version 2.4.x, scala 2.11). 
You will also need to download the 
[crime.csv](https://www.kaggle.com/AnalyzeBoston/crimes-in-boston/data#crime.csv) 
and [offense_codes.csv](https://www.kaggle.com/AnalyzeBoston/crimes-in-boston/data#offense_codes.csv).
Then clone the repository:
```console
$ git clone git@github.com:sbordya/boston_crimes_map_bordea.git
```
Navigate to the project folder and prepare a jar file with dependencies
(you will be able to see it under the path 
`<path_to_project>/target/scala-2.11/boston_crimes_map_bordea-assembly-0.0.1.jar`):
```console
$ sbt assembly
```
Now you are ready to run the program:
```console
$ <path_to_spark>/bin/spark-submit --master local[*] --class com.example.BostonCrimesMap <path_to_project>/target/scala-2.11/boston_crimes_map_bordea-assembly-0.0.1.jar <path_to_crime.csv> <path_to_offense_codes.csv> <path_to_output_folder>
```

If you want to see the graph of dependencies, you can run:
```console
$ sbt dependencyBrowseGraph
```
### Project Goals

This program was created during the 
[data engineer](https://otus.ru/lessons/data-engineer/?int_source=courses_catalog&int_term=data-science) 
course on [otus.ru](https://otus.ru/). 
