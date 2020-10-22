FROM python:3.6
MAINTAINER Yujing Wu <yujing.wu@wri.org>
#Note this script was originally developed by Yujing Wu <yujing.wu@wri.org>

ARG NAME=soc_049
ENV NAME ${NAME}

# install core libraries
RUN apt-get update
RUN apt-get install -y cron

# install application libraries
RUN pip install --upgrade pip
RUN pip install python-dateutil==2.6.1 \
    pytz==2017.3 \
    bs4==0.0.1 \
    urllib3==1.25.10 \
    carto==1.11.1 \
    requests==2.23.0 \
    pandas==0.25.3
RUN pip install -e git+https://github.com/resource-watch/cartosql.py.git#egg=cartosql

# Copy the application folder inside the container
RUN mkdir -p /opt/$NAME/data
WORKDIR /opt/$NAME/
COPY contents/ .
COPY time.cron .
VOLUME ./data

# Set user for container security
RUN useradd $NAME
RUN chown -R $NAME:$NAME .
USER $NAME

# Launch script
CMD ["python", "main.py"]
