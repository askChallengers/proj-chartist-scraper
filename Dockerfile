FROM python:3.9

WORKDIR /app

# Copy the necessary files to the working directory
COPY . .

# Set PYTHONPATH
ENV PYTHONPATH "${PYTHONPATH}:/app"

# Debug: List files in /app and print PYTHONPATH

RUN ls -R /app

RUN echo $PYTHONPATH

# Install dependencies
RUN pip install poetry
RUN poetry export --without-hashes --format=requirements.txt > requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Install Chrome
RUN apt-get update && apt-get install -y wget
RUN wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb 
RUN dpkg -i google-chrome-stable_current_amd64.deb || ( apt-get install -f -y && dpkg -i google-chrome-stable_current_amd64.deb)