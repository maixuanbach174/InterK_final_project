FROM python:3.13-bookworm

# set work directory
WORKDIR /app

# set env variables
COPY . .

RUN pip install .

CMD ["/bin/bash", "-c", "while true; do sleep 3600; done"]

# copy project
