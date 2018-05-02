FROM continuumio/miniconda3

# Install necessary libraries
RUN conda install pyodbc
RUN apt-get update -y
RUN apt-get install -y build-essential unixodbc-dev unixodbc-bin unixodbc libpq-dev

# Configure postgresql drivers
RUN wget https://ftp.postgresql.org/pub/odbc/versions/src/psqlodbc-09.02.0100.tar.gz
RUN gunzip psqlodbc-09.02.0100.tar.gz
RUN tar xvf psqlodbc-09.02.0100.tar
RUN cd psqlodbc-09.02.0100 && sh ./configure --with-unixodbc && make && make install

# Provide name of container
ARG NAME=com_009_connector
ENV NAME ${NAME}

# Copy the application folder inside the container
RUN mkdir -p /opt/$NAME/data
WORKDIR /opt/$NAME/
COPY contents/ .

# Set up ODBC driver info
RUN mv /opt/$NAME/odbcinst.ini /etc/odbcinst.ini

# Restrict permissions
RUN useradd -r $NAME
RUN chown -R $NAME:$NAME /opt/$NAME
VOLUME /opt/$NAME/data
#USER $NAME

CMD ["python", "main.py"]
