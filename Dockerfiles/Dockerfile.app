FROM python:3.11

WORKDIR /app

# copy project
COPY . .
RUN pip install .

ENTRYPOINT ["udata-search-service", "run", "--host=0.0.0.0"]
