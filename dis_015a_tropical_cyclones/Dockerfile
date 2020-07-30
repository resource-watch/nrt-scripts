FROM python:3.6
MAINTAINER Taufiq Rashid <taufiq.rashid@wri.org>
#Note this script was originally developed by Taufiq Rashid <taufiq.rashid@wri.org>

# install application libraries
RUN pip install --upgrade pip && \
	carto==1.8.4 cartoframes==0.9.2 \
	geopandas==0.8.0 \
	glob2==0.7 \
    requests==2.22.0 fiona==1.8.13
RUN pip install -e git+https://github.com/resource-watch/cartosql.py.git#egg=cartosql 

# set name
ARG NAME=dis_015a
ENV NAME ${NAME}

# copy the application folder inside the container
RUN mkdir -p /opt/$NAME/data
WORKDIR /opt/$NAME/
COPY contents/ .
VOLUME ./data

# add user
RUN groupadd -r $NAME && useradd -r -g $NAME $NAME
RUN chown -R $NAME:$NAME /opt/$NAME/
USER $NAME

CMD ["python", "main.py"]
