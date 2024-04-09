FROM python:3.11.7-bookworm

RUN groupadd -g 999 python && \
    useradd -r -u 999 -g python python && mkdir -p /home/python && chown python:python /home/python/
RUN apt-get update \
    && apt-get -y install libpq-dev gcc python3 uvicorn && rm -rf /var/lib/apt/lists/* && apt-get autoremove \
    && pip3 install psycopg2 
WORKDIR /app
USER 999
COPY . . 

RUN pip3 install --upgrade pip && pip3 install -r requeriments.txt

##Run only production
#CMD [ "gunicorn", "--bind", "0.0.0.0:5000", "app:create_app()" ]


CMD ["python3" , "main.py"]