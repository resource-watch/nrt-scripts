FROM python:3.6
MAINTAINER Amelia Snyder <amelia.snyder@wri.org>

# install core libraries
RUN apt-get update
RUN pip install -U pip

# install application libraries
RUN pip install bs4
RUN pip install -e git+https://github.com/fgassert/cartosql.py.git#egg=cartosql
RUN pip install pandas
RUN pip install shapely

# set name
ARG NAME=nrt-script
ENV NAME ${NAME}

# copy the application folder inside the container
RUN mkdir -p /opt/$NAME/data
WORKDIR /opt/$NAME/
COPY contents/ .

RUN useradd $NAME
RUN chown -R $NAME:$NAME .
USER $NAME

CMD ["python", "main.py"]