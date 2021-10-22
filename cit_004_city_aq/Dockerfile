FROM python:3.7
MAINTAINER Taufiq Rashid <taufiq.rashid@wri.org>
#Note this script was originally developed by Taufiq Rashid <taufiq.rashid@wri.org>

# install core libraries
RUN apt-get update

# install application libraries
RUN pip install requests==2.22.0
RUN pip install -e git+https://github.com/resource-watch/cartosql.py.git#egg=cartosql
RUN pip install pandas==0.25.3
RUN pip install numpy==1.16.5
RUN pip install shapely==1.7.1
RUN pip install cartoframes==1.1.0

# set name
ARG NAME=cit_004_city_aq
ENV NAME ${NAME}

# copy the application folder inside the container
RUN mkdir -p /opt/$NAME/data
WORKDIR /opt/$NAME/
COPY contents .
VOLUME /opt/$NAME/data

RUN useradd $NAME
RUN chown -R $NAME:$NAME .
USER $NAME

CMD ["python", "main.py"]
