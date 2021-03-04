FROM python:3.6
MAINTAINER Yujing Wu <yujing.wu@wri.org>
#Note this script was originally developed by Yujing Wu <yujing.wu@wri.org>

# install core libraries
RUN apt-get update
RUN pip install -U pip

# install application libraries
RUN pip install carto==1.11.1
RUN pip install multiprocessing
RUN pip install requests
RUN pip install numpy
RUN pip install pandas

# set name
ARG NAME=bio_007_rw2_wdpa
ENV NAME ${NAME}

# copy the application folder inside the container
RUN mkdir -p /opt/$NAME/data
WORKDIR /opt/$NAME/
COPY contents .

RUN useradd $NAME
RUN chown -R $NAME:$NAME .
USER $NAME

CMD ["python", "main.py"]