FROM python:3.6
MAINTAINER Taufiq Rashid <taufiq.rashid@wri.org>
#Note this script was originally developed by Taufiq Rashid <taufiq.rashid@wri.org>

# set name
ARG NAME=dis_015a_tropical_cyclones
ENV NAME ${NAME}

# install core libraries
RUN apt-get update
RUN apt-get install -y cron

# install application libraries
RUN pip install --upgrade pip  
RUN pip install pandas==0.25.3 numpy==1.18.1
RUN pip install geopandas==0.9.0 \
	glob2==0.7 \
    requests==2.22.0 fiona==1.8.13
RUN pip install -U carto cartoframes==1.0.4
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
