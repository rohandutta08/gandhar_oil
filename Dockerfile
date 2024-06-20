FROM python:3.8-slim-buster

WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends gcc \
        g++ \
        tzdata \
        python3-setuptools \
        libpq-dev \
        python3-dev \
        && \
    apt install make && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*


COPY requirements.txt /app/requirements.txt
RUN pip install -r requirements.txt

ADD . /app/

ENV PATH="/usr/local/bin:${PATH}"
RUN python3 setup.py install

#RUN chmod +x /app/docker_start.sh
#ENTRYPOINT ["/app/docker_start.sh"]