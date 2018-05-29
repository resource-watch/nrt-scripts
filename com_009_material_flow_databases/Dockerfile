FROM continuumio/miniconda3
MAINTAINER Nathan Suberi <nathan.suberi@wri.org>

# Provide name of container
ARG NAME

# Install necessary libraries
RUN apt-get update -y && apt-get install -y build-essential unixodbc-dev unixodbc-bin unixodbc libpq-dev
RUN conda update -n base conda && conda install pyodbc pandas
RUN pip install cartoframes && pip uninstall -y tqdm && pip install tqdm==4.20.0

# Configure postgresql drivers
RUN wget https://ftp.postgresql.org/pub/odbc/versions/src/psqlodbc-09.02.0100.tar.gz
RUN gunzip psqlodbc-09.02.0100.tar.gz
RUN tar xvf psqlodbc-09.02.0100.tar
RUN cd psqlodbc-09.02.0100 && sh ./configure --with-unixodbc && make && make install

# Copy the application folder inside the container
RUN mkdir -p /opt/$NAME/data
VOLUME /opt/$NAME/data
WORKDIR /opt/$NAME/
COPY contents/ .

# Set up ODBC driver info
RUN mv /opt/$NAME/odbcinst.ini /etc/odbcinst.ini

# Restrict permissions
RUN useradd -r $NAME
RUN chown -R $NAME:$NAME .
#USER $NAME

CMD ["python", "main.py"]
