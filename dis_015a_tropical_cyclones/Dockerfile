FROM python:3.9
MAINTAINER Weiqi Zhou <weiqi.zhou@wri.org>
# Note this script was originally developed by Taufiq Rashid <taufiq.rashid@wri.org>

# set name
ARG NAME=nrt-script
ENV NAME ${NAME}

# install core libraries
RUN apt-get update
RUN apt-get install -y cron

# install application libraries
RUN pip install --upgrade pip  
RUN pip install pandas numpy
RUN pip install geopandas glob2 requests fiona
RUN pip install -U carto cartoframes
RUN pip install LMIPy==0.2.8
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
