spark-cep
=============

This repository is for Complex Event Processing with Spark


LOAD DATA INTO HDFS
-------------------

	hadoop fs -put README.md
	hadoop fs -put sparkrecipesdata
	

RUN SPARK from com.sparkrecipes.* package
-----------------------------------------

	spark-submit --class com.sparkrecipes.kmeans.KMeans --master yarn target/spark-recipes-1.0.jar sparkrecipesdata/ml/kmeans_data.txt 5 2

