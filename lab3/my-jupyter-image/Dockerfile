# Use the Jupyter Data Science Notebook as the base image
FROM jupyter/datascience-notebook

# Set the working directory to /home/jovyan - the default for Jupyter images
WORKDIR /home/jovyan

EXPOSE 8888

# Avoid prompts from apt during the build process
ENV DEBIAN_FRONTEND=noninteractive

COPY . .

# Install the latest AWS CLI version 2
USER root

# Update and install necessary packages
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    unzip \
    bzip2 \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install the AWS CLI tools to your image
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -rf awscliv2.zip ./aws
USER jovyan
