FROM python:3.6
MAINTAINER Nathan Suberi <nathan.suberi@wri.org>
ARG NAME=python-script
ENV NAME ${NAME}

# install core libraries
RUN apt-get update
RUN apt-get install -y cron
# install application libraries, comment out for vector only connectors
#RUN apt-get install -y gdal-bin libgdal-dev

COPY contents/requirements.txt .
# install application libraries
#RUN pip install numpy
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy the application folder inside the container
RUN mkdir -p /opt/$NAME/data
WORKDIR /opt/$NAME/
COPY contents/ .
COPY time.cron .
VOLUME ./data

# Launch script
CMD ["python", "main.py"]
