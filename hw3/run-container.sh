docker run -v ~/de300/hw3:/tmp/ml -it \
           -p 8888:8888 \
           --name spark-sql-container \
	   pyspark-image
