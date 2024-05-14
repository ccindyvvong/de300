docker build -t pyspark-image . 

docker run -v ~/de300/lab6/word-count:/tmp/wc-demo -it \
	   -p 8888:8888 \
           --name wc-container \
	   pyspark-image
