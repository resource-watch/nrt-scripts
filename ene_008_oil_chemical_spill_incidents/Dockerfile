FROM python:3.6
MAINTAINER Nathan Suberi <nathan.suberi@wri.org>
ARG NAME=ene_008
ENV NAME ${NAME}

# install core libraries
RUN apt-get update
RUN apt-get install -y cron

RUN pip install -e git+https://github.com/fgassert/cartosql.py.git#egg=cartosql

# Copy the application folder inside the container
RUN mkdir -p /opt/$NAME/data
WORKDIR /opt/$NAME/
COPY contents/ .
COPY time.cron .
VOLUME ./data

# Set user for container security
RUN groupadd -r $NAME && useradd -r -g $NAME $NAME
RUN chown -R $NAME:$NAME /opt/$NAME/
USER $NAME

# Launch script
CMD ["python", "main.py"]
