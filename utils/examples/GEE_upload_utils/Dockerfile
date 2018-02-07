FROM python:3.5
MAINTAINER Raul <raul.requero@vizzuality.com>
ARG NAME=python-script
ENV NAME ${NAME}

RUN apt-get update && \
   apt-get install -y bash git openssl build-essential \
   libffi-dev libxml2 libxslt-dev

USER root

RUN groupadd -r $NAME && useradd -r -g $NAME $NAME

RUN easy_install pip && pip install --upgrade pip
RUN pip install virtualenv

RUN mkdir -p /opt/$NAME
COPY requirements.txt /opt/$NAME/requirements.txt
RUN cd /opt/$NAME && pip install -r requirements.txt

COPY main.py /opt/$NAME/main.py

# Copy the application folder inside the container
WORKDIR /opt/$NAME

COPY ./src /opt/$NAME/src
COPY ./data /opt/$NAME/data

RUN chown $NAME:$NAME /opt/$NAME
USER $NAME

VOLUME /opt/$NAME/data

# Launch script
CMD ["python", "main.py"]