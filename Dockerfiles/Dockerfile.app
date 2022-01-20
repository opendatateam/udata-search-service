FROM python:3.9

WORKDIR /app

# install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# copy project
COPY . .

RUN flask init-es

ENTRYPOINT ["bash", "gunicorn.sh"]