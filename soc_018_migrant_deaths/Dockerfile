FROM python:3.9
LABEL maintainer="Weiqi Zhou <weiqi.zhou@wri.org>"
#Note this script was originally developed by Nathan Suberi <nathan.suberi@wri.org>

# set name
ARG NAME=nrt-script
ENV NAME=${NAME}

# install core libraries
RUN apt-get update

# install application libraries
RUN pip install --upgrade pip && pip install requests
RUN pip install -e git+https://github.com/resource-watch/cartosql.py.git#egg=cartosql
RUN pip install pandas==2.2.2
RUN pip install openpyxl
RUN pip install bs4

# Copy the application folder inside the container
RUN mkdir -p /opt/$NAME/data
WORKDIR /opt/$NAME/
COPY contents/ .

# Set user for container security
RUN useradd $NAME
RUN chown -R $NAME:$NAME .
USER $NAME

# Launch script
CMD ["python", "main.py"]
