FROM python:3.6
MAINTAINER Yujing Wu <yujing.wu@wri.org>
#Note this script was originally developed by Yujing Wu <yujing.wu@wri.org>

# install core libraries
RUN apt-get update
RUN pip install --upgrade pip

# install application libraries
RUN pip install carto==1.11.1
RUN pip install -e git+https://github.com/resource-watch/cartosql.py.git#egg=cartosql
RUN pip install requests
RUN pip install numpy
RUN pip install pandas
RUN pip install python-rapidjson
RUN pip install geopandas==0.9.0

# set name
ARG NAME=bio_007_wdpa
ENV NAME ${NAME}

# copy the application folder inside the container
RUN mkdir -p /opt/$NAME/data
WORKDIR /opt/$NAME/
COPY contents .

RUN useradd $NAME
RUN chown -R $NAME:$NAME .
USER $NAME

CMD ["python", "main.py"]
