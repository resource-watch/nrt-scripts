FROM python:3.6-alpine
MAINTAINER Taufiq Rashid <taufiq.rashid@wri.org>
#Note this script was originally developed by Taufiq Rashid <taufiq.rashid@wri.org>

# install core libraries
RUN apk update
RUN apk add git

# install application libraries
RUN pip install requests==2.22.0
RUN pip install -e git+https://github.com/resource-watch/cartosql.py.git#egg=cartosql
RUN pip install pandas==0.25.3

# set name
ARG NAME=cit_004_city_aq
ENV NAME ${NAME}

# copy the application folder inside the container
RUN mkdir -p /opt/$NAME/data
WORKDIR /opt/$NAME/
COPY contents .
VOLUME ./data

RUN adduser -D $NAME
RUN chown $NAME:$NAME .
USER $NAME

CMD ["python", "main.py"]
