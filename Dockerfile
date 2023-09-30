# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set environment variables
# Use unbuffered mode to receive logs from the output, it’s also recommended when running Python within Docker containers
ENV PYTHONUNBUFFERED 1
ENV NAME World  # This can be removed if not needed

# Set the working directory in the container to /app
WORKDIR /app

# Installing OS dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        default-libmysqlclient-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# We copy just the requirements.txt first to leverage Docker cache
COPY ./requirements.txt /app/requirements.txt

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the current directory contents into the container at /app
COPY . /app

# Make port 8000 available to the world outside this container
EXPOSE 8000

# Run api.py when the container launches
CMD ["uvicorn", "api:api", "--host", "0.0.0.0", "--port", "8000"]
