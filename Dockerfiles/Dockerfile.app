FROM python:3.9

WORKDIR /app

# install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# copy project
COPY . .

RUN FLIT_ROOT_INSTALL=1 make install

ENTRYPOINT ["udata-search-service", "run", "--host=0.0.0.0"]
