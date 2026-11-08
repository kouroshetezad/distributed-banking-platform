FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    WORKDIR=/app

WORKDIR /app

COPY platform /app/platform
COPY tests /app/tests
COPY requirements.txt /app/requirements.txt

RUN mkdir -p /data
VOLUME ["/data"]

COPY docker/run.sh /usr/local/bin/run
# Tolerate a CRLF checkout: a \r in the shebang breaks the entrypoint.
RUN sed -i 's/\r$//' /usr/local/bin/run && chmod +x /usr/local/bin/run

ENTRYPOINT ["/usr/local/bin/run"]
