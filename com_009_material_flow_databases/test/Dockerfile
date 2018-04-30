FROM continuumio/miniconda3

RUN conda install pyodbc

RUN apt-get update -y
RUN apt-get install -y build-essential
RUN wget https://ftp.postgresql.org/pub/odbc/versions/src/psqlodbc-09.02.0100.tar.gz
RUN gunzip psqlodbc-09.02.0100.tar.gz
RUN tar xvf psqlodbc-09.02.0100.tar
RUN cd psqlodbc-09.02.0100 && sh ./configure --with-unixodbc && make && make install
