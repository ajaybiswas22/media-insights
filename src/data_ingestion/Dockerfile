FROM python:3.13-alpine

WORKDIR /app

# Copy requirements.txt first to leverage caching 
COPY requirements.txt .

RUN apk update && apk add --no-cache \
    musl-dev build-base gcc gfortran openblas-dev \
    linux-headers
RUN pip install --upgrade pip setuptools wheel
 
# Install dependencies 
RUN pip install --no-cache-dir -r requirements.txt
ENV PYTHONPATH="${PYTHONPATH}:/app/utils"

CMD ["tail", "-f", "/dev/null"]