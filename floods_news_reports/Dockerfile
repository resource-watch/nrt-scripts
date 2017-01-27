FROM python:2.7-alpine
MAINTAINER Raul <raul.requero@vizzuality.com>
ARG NAME=python-script
ENV NAME ${NAME}

RUN apk update && apk upgrade && \
   apk add --no-cache --update bash git openssl-dev build-base alpine-sdk \
   libffi-dev libxml2-dev libxslt-dev

RUN addgroup $NAME && adduser -s /bin/bash -D -G $NAME $NAME

RUN easy_install pip && pip install --upgrade pip
RUN pip install virtualenv

RUN mkdir -p /opt/$NAME
RUN cd /opt/$NAME && virtualenv venv && source venv/bin/activate
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
ENTRYPOINT ["python", "main.py"]