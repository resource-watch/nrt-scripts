FROM python:3.9
MAINTAINER Weiqi Zhou <weiqi.zhou@wri.org>
#Note this script was originally developed by Nathan Suberi <nathan.suberi@wri.org>

ARG NAME=soc_018
ENV NAME ${NAME}

# install core libraries
RUN apt-get update

# install application libraries
RUN pip install --upgrade pip && pip install requests==2.28.2
RUN pip install -e git+https://github.com/resource-watch/cartosql.py.git#egg=cartosql
RUN pip install pandas==1.5.3
RUN pip install openpyxl

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