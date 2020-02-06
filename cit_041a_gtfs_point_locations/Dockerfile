FROM python:3.6
MAINTAINER Kristine Lister <kristine.lister@wri.org>

# install core libraries
RUN apt-get update
RUN pip install -U pip

# install application libraries
RUN pip install --upgrade pip
RUN pip install requests
RUN pip install -e git+https://github.com/resource-watch/cartosql.py.git#egg=cartosql
RUN pip install numpy
RUN pip install requests
RUN pip install pandas numpy
RUN pip install boto3
RUN pip install shapely
RUN pip install carto cartoframes


# set name
ARG NAME=nrt-script
ENV NAME ${NAME}

# copy the application folder inside the container
RUN mkdir -p /opt/$NAME/data
WORKDIR /opt/$NAME/
COPY contents/ .

RUN useradd -r $NAME
RUN chown -R $NAME:$NAME /opt/$NAME
USER $NAME

CMD ["python", "main.py"]
