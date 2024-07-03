FROM python:3.9
LABEL maintainer="Weiqi Zhou <weiqi.zhou@wri.org>"
#Note this script was originally developed by Taufiq Rashid <taufiq.rashid@wri.org>

# install core libraries
RUN apt-get update

# install application libraries
RUN pip install requests==2.32.3
RUN pip install -e git+https://github.com/resource-watch/cartosql.py.git#egg=cartosql
RUN pip install pandas==2.2.2
RUN pip install numpy==1.26.4
RUN pip install shapely==2.0.4
RUN pip install cartoframes==1.2.5

# set name
ARG NAME=nrt-script
ENV NAME=${NAME}

# copy the application folder inside the container
RUN mkdir -p /opt/$NAME/data
WORKDIR /opt/$NAME/
COPY contents .
VOLUME ./data

RUN useradd $NAME
RUN chown -R $NAME:$NAME .
USER $NAME

CMD ["python", "main.py"]
