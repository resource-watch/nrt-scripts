FROM python:3.9
LABEL maintainer="Weiqi Zhou <weiqi.zhou@wri.org>"
# Note this script was originally developed by Nathan Suberi <nathan.suberi@wri.org> and maintained by Amelia Snyder

# install core libraries
RUN apt-get update
# RUN pip install -U pip
# RUN apt-get install -y cron

# install application libraries
RUN pip install requests==2.31.0
RUN pip install simplejson==3.19.1
RUN pip install -e git+https://github.com/resource-watch/cartosql.py.git#egg=cartosql

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
