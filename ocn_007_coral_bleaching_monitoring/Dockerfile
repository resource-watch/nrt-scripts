FROM python:3.9
LABEL maintainer="Taufiq Rashid <taufiq.rashid@wri.org>"
#Note this script was originally developed by Taufiq Rashid <taufiq.rashid@wri.org>

# install core libraries
RUN apt-get update
RUN pip install -U pip

# install application libraries
RUN apt-get install -y gdal-bin libgdal-dev
RUN pip install oauth2client==4.1.3
RUN pip install google-api-python-client==2.135.0
RUN pip install -e git+https://github.com/resource-watch/eeUtil#egg=eeUtil
RUN pip install requests==2.32.3
RUN pip install numpy==1.26.4
RUN pip install Cython==3.0.10
RUN pip install GDAL==3.6.2
RUN apt-get -y install python-gdal

# set name
ARG NAME=nrt-script
ENV NAME=${NAME}

# copy the application folder inside the container
RUN mkdir -p /opt/$NAME/data
WORKDIR /opt/$NAME/
COPY contents/ .

RUN useradd -r $NAME
RUN chown -R $NAME:$NAME /opt/$NAME
USER $NAME

CMD ["python", "main.py"]
