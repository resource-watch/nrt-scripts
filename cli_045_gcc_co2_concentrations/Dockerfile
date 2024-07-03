FROM python:3.9
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
    wget==3.2
RUN pip install pandas==2.2.2 numpy==1.26.4
RUN pip install carto==1.8.4 cartoframes==0.9.2
RUN pip install -e git+https://github.com/resource-watch/cartosql.py.git#egg=cartosql

# Copy the application folder inside the container
RUN mkdir -p /opt/$NAME/data
WORKDIR /opt/$NAME/
COPY contents/ .
COPY time.cron .
VOLUME ./data

# Set user for container security
RUN useradd -r $NAME
RUN chown -R $NAME:$NAME /opt/$NAME
#USER $NAME

# Launch script
CMD ["python", "main.py"]
