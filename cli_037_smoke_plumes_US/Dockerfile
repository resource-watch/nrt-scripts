FROM python:3.6
MAINTAINER Amelia Snyder <amelia.snyder@wri.org>
#Note this script was originally developed by Nathan Suberi <nathan.suberi@wri.org>

# install application libraries
RUN pip install --upgrade pip && \
    pip install -e git+https://github.com/resource-watch/cartosql.py.git#egg=cartosql && \
    pip install requests==2.22.0 fiona==1.8.13

# set name
ARG NAME=cli_037
ENV NAME ${NAME}

# copy the application folder inside the container
RUN mkdir -p /opt/$NAME/data
WORKDIR /opt/$NAME/
COPY contents/ .
VOLUME ./data

# add user
RUN groupadd -r $NAME && useradd -r -g $NAME $NAME
RUN chown -R $NAME:$NAME /opt/$NAME/
USER $NAME

CMD ["python", "main.py"]
