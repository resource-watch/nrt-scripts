FROM python:3.9
LABEL maintainer="Weiqi Zhou <weiqi.zhou@wri.org>"
#Note this script was originally developed by Nathan Suberi <nathan.suberi@wri.org> and Amelia Snyder <amelia.snyder@wri.org>

# install core libraries
RUN apt-get update
# install application libraries
RUN pip install --upgrade pip
RUN pip install requests
RUN pip install -e git+https://github.com/resource-watch/cartosql.py.git#egg=cartosql
RUN pip install shapely
RUN pip install numpy
RUN pip install pandas
RUN pip install python-dateutil
RUN pip install backoff
RUN pip install httpx

# set name
ARG NAME=nrt-script
ENV NAME=${NAME}

# copy the application folder inside the container
RUN mkdir -p /opt/$NAME/data
WORKDIR /opt/$NAME/
COPY contents/ .
VOLUME ./data

RUN useradd $NAME
RUN chown -R $NAME:$NAME .
USER $NAME

CMD ["python", "main.py"]
