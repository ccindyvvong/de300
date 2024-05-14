docker run -v /de300/lab6/:/tmp/lab6 -it \
           -p 8888:8888 \
           --name spark-sql-container \
	   pyspark-image
