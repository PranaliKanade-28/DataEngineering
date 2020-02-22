# DataEngineering

# Data Pipeline

`Data Pipeline basically has following components`

1) Rest Api 
2) Amazon EMR Spark Cluster 
3) Amazon Redshift Datawarehouse

# Pipeline Functionality

Python application parses the json responses that we get when we hit a Rest Api. This python application will run inside the spark cluster. The parsed 
data will be stored in spark dataframes. Any required transformations can be applied through spark dataframe transformations. Once the data is
transformed the data is loaded from spark dataframes to amazon Redshift datawarehouse in aws. By using spark cluster we can easily process huge amounts
of data in a cost efficient way.
