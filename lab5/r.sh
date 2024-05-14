# Build PostgreSQL Docker image
echo "Building PostgreSQL Docker image from dockerfile-postgresql"
docker build -f dockerfiles/dockerfile-postgresql -t postgresql-image .

# Build Jupyter Docker image
echo "Building Jupyter Docker image from dockerfile-jupyter"
docker build -f dockerfiles/dockerfile-jupyter -t jupyter-image .

# Run PostgreSQL container with volume and network setup
echo "Starting PostgreSQL container"
docker run -d --network lab5-database \
           --name container-postgres \
           -v lab5-database:/var/lib/postgresql/data \
           -p 5432:5432 \
           postgresql-image

# Run Jupyter container with volume and network setup
echo "Starting Jupyter container"
docker run -it --network lab5-database \
           --name container-jupyter \
           -v ./src:/app/src \
           -v ./staging_data:/app/staging_data \
           -p 8888:8888 \
           jupyter-image
