FROM python:3.9

RUN pip3 install kafka-python==2.0.2 \
                 elasticsearch==7.15.0

WORKDIR /srv/consumer/

COPY consumer.py /srv/consumer/consumer.py

ENTRYPOINT ["python", "consumer.py"]
