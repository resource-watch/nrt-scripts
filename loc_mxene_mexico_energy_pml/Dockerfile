FROM python:3.6
MAINTAINER Eduardo Castillero <eduardo.castillero@wri.org>
#Note this script was originally developed by Mexico's energy team and adapted to NRT form by Eduardo Castillero

ARG NAME=loc_mxene
ENV NAME ${NAME}

# install core libraries
RUN apt-get update
RUN apt-get install -y cron

# install application libraries
RUN pip install -e git+https://github.com/resource-watch/cartosql.py.git#egg=cartosql
RUN pip install --upgrade pip && pip install \
    requests==2.24.0 \
    numpy==1.19.4 \
    pandas==1.1.4 \
    cartoframes==1.1.0 \
    libxml2-python3==2.9.5 \
    lxml==4.6.2 \
    beautifulsoup4==4.9.3 

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
