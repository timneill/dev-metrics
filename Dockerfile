# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /usr/src/app

# Copy the current directory contents into the container at /usr/src/app
COPY . .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

ENV GH_TOKEN=GITHUB_TOKEN
ENV GH_ORG=GITHUB_TARGET_ORG
ENV GH_BASE_URL=GITHUB_BASE_URL
ENV DB_PATH=DATABASE_PATH

# Run open_prs.py when the container launches
CMD ["python", "./dev-metrics/__main__.py"]
