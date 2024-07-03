FROM python:3.9
LABEL maintainer="Weiqi Zhou <weiqi.zhou@wri.org>"
# Note this script was originally developed by Nathan Suberi <nathan.suberi@wri.org> and maintained by Amelia Snyder

# install core libraries
RUN apt-get update
RUN pip install -U pip

# install application libraries
RUN pip install --upgrade pip && pip install requests
RUN pip install -e git+https://github.com/resource-watch/cartosql.py.git#egg=cartosql

# Env
ARG NAME=nrt-script
ENV NAME=${NAME}

# Copy the application folder inside the container
RUN mkdir -p /opt/$NAME/data
WORKDIR /opt/$NAME/
COPY contents/ .
COPY time.cron .
VOLUME ./data

# Set user for container security
USER root
RUN useradd $NAME
RUN chown -R $NAME:$NAME .
USER $NAME

# Launch script
CMD ["python", "main.py"]
