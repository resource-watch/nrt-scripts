FROM python:3.6
MAINTAINER Amelia Snyder <amelia.snyder@wri.org>

# install core libraries
RUN apt-get update && pip install -U pip

# install application libraries
RUN pip install requests
RUN pip install pandas==0.24.2

# set name
ARG NAME=nrt-script
ENV NAME ${NAME}

# copy the application folder inside the container
RUN mkdir -p /opt/$NAME/data
WORKDIR /opt/$NAME/
COPY contents/ .

RUN useradd -r $NAME
RUN chown -R $NAME:$NAME /opt/$NAME
VOLUME /opt/$NAME/data
USER $NAME

CMD ["python", "main.py"]
