FROM python:3.9
MAINTAINER Weiqi Zhou <weiqi.zhou@wri.org>
#Note this script was originally developed by Francis Gassert <fgassert@wri.org>

# install core libraries
RUN apt-get update
RUN pip install -U pip

# install application libraries
RUN pip install requests
RUN pip install -e git+https://github.com/resource-watch/cartosql.py.git#egg=cartosql
RUN pip install boto3
RUN pip install ndjson

# set name
ARG NAME=nrt-script
ENV NAME ${NAME}

# copy the application folder inside the container
RUN mkdir -p /opt/$NAME/data
WORKDIR /opt/$NAME/
COPY contents/ .
VOLUME ./data

# Set user for container security
RUN useradd $NAME
RUN chown -R $NAME:$NAME .
USER $NAME

CMD ["python", "main.py"]
