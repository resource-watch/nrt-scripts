FROM python:3.6
MAINTAINER Nathan Suberi <nathan.suberi@wri.org>
ARG NAME=dis_003
ENV NAME ${NAME}

# install core libraries
RUN apt-get update
RUN apt-get install -y cron

# install application libraries
RUN pip install --upgrade pip && pip install \
    requests \
    cartoframes
RUN pip install -e git+https://github.com/fgassert/cartosql.py.git#egg=cartosql

# Copy the application folder inside the container
RUN mkdir -p /opt/$NAME/data
WORKDIR /opt/$NAME/
COPY contents/ .
COPY time.cron .

# Set user for container security
RUN useradd $NAME
RUN chown -R $NAME:$NAME .
#USER $NAME

# Launch script
CMD ["python", "main.py"]
