# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set environment variables
# Use unbuffered mode to receive logs from the output, it’s also recommended when running Python within Docker containers
ENV PYTHONUNBUFFERED 1

# Set the working directory in the container to /app
WORKDIR /app

# Installing OS dependencies
RUN apt-get update && apt-get upgrade -y
RUN apt-get install -y --no-install-recommends \
	gcc \
	curl \
	wget \
	default-libmysqlclient-dev \
	&& apt-get clean \
	&& rm -rf /var/lib/apt/lists/*

# We copy just the requirements.txt first to leverage Docker cache
# COPY ./requirements.txt /app/requirements.txt
COPY ./requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the current directory contents into the container at /app
COPY app/ .
COPY cert/ ./cert

# Make port 8000 available to the world outside this container
EXPOSE 8000

# Generate private key
# RUN openssl genrsa -out cert/privatekey.pem 2048
# RUN openssl req -new -x509 -key /privatekey.pem -out /publiccert.pem -days 365 \
#     -subj "/C=DE/ST=Berlin/L=Berlin/O=DataScientest/OU=DE_Jul_23/CN=127.0.0.0"

# RUN openssl genrsa -out cert/privatekey.key 2048
# RUN openssl req -new -x509 -key cert/privatekey.key -out cert/publiccert.crt -days 365 -subj "/C=DE/ST=Berlin/L=Berlin/O=DataScientest/OU=DE_Jul_23/CN=127.0.0.0"


# Run api.py when the container launches
CMD ["uvicorn", "api:api", "--reload", "--host", "0.0.0.0", "--port", "8000"]

# NOT WORKING YET...
# CMD ["uvicorn", "api_jwt:api", "--ssl-keyfile", "./cert/key-no-pass.pem", "--ssl-certfile", "./cert/cert.pem", "--host", "0.0.0.0", "--port", "8000"]
# CMD ["uvicorn", "api_jwt:api", "--ssl-keyfile", "./cert/key-no-pass.pem", "--ssl-certfile", "./cert/cert.pem"]
# CMD ["uvicorn", "api_jwt:api", "--ssl-keyfile", "./cert/key-no-pass.pem", "--ssl-certfile", "./cert/cert.pem", "--host", "127.0.0.1", "--port", "443"]
