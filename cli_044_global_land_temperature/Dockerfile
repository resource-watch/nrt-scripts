FROM python:3.6
LABEL maintainer="Taufiq Rashid <taufiq.rashid@wri.org>"
#Note this script was originally developed by Taufiq Rashid <taufiq.rashid@wri.org>

# set name
ARG NAME=nrt-script
ENV NAME=${NAME}

# install core libraries
RUN apt-get update
RUN apt-get install -y cron

# install application libraries
RUN pip install --upgrade pip
RUN pip install requests==2.32.0 \
    python-dateutil==2.6.1 \
    pytz==2017.3 \
    bs4==0.0.1
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
