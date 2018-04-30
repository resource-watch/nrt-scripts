FROM python:3.6
MAINTAINER Nathan Suberi <nathan.suberi@wri.org>

RUN apt-get update -y

# Install core libraries for ODBC connection
RUN apt-get install -y unixodbc-dev unixodbc-bin unixodbc

# https://github.com/mkleehammer/pyodbc
RUN pip install --upgrade pip && pip install pyodbc

## Some attempts at manually installing the drivers fail
## https://blog.csdn.net/jollypigclub/article/details/46490541
## https://www.cnblogs.com/he11o-liu/p/7503232.html
## https://odbc.postgresql.org/docs/unix-compilation.html

#RUN apt-get install -y build-essential
#RUN cd /home
#RUN wget https://ftp.postgresql.org/pub/odbc/versions/src/psqlodbc-09.02.0100.tar.gz
#RUN gunzip psqlodbc-09.02.0100.tar.gz
#RUN tar xvf psqlodbc-09.02.0100.tar
#RUN cd psqlodbc-09.02.0100 && sh ./configure --with-unixodbc && make && make install

# set name
ARG NAME=nrt-script
ENV NAME ${NAME}

# copy the application folder inside the container
RUN mkdir -p /opt/$NAME/data
WORKDIR /opt/$NAME/
COPY contents/ .

# Set up ODBC driver info
RUN mv /opt/$NAME/odbcinst.ini /etc/odbcinst.ini

RUN useradd -r $NAME
RUN chown -R $NAME:$NAME /opt/$NAME
VOLUME /opt/$NAME/data
#USER $NAME

CMD ["python", "main.py"]
