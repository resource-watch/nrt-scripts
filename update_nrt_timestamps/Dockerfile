FROM python:3.9
LABEL maintainer="Weiqi Zhou <weiqi.zhou@wri.org>"
#Note this script was originally developed by Amelia Snyder

# install core libraries
RUN apt-get update && pip install -U pip

# install application libraries
RUN pip install oauth2client==3.0.0
RUN pip install google-api-python-client==1.12.8
RUN pip install earthengine-api==0.1.234

# set name
ARG NAME=nrt-script
ENV NAME=${NAME}

# copy the application folder inside the container
RUN mkdir -p /opt/$NAME/data
WORKDIR /opt/$NAME/
COPY contents/ .

RUN useradd -r $NAME
RUN chown -R $NAME:$NAME /opt/$NAME
VOLUME /opt/$NAME/data
USER $NAME

CMD ["python", "main.py"]
