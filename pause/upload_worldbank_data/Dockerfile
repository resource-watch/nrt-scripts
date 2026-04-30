FROM python:3.9
LABEL maintainer="Weiqi Zhou <weiqi.zhou@wri.org>"
# Note this script was originally developed by Amelia Snyder

# install core libraries
RUN apt-get update 
RUN pip install -U pip

# install application libraries
RUN pip install pandas==2.2.2
RUN pip install boto3==1.35.20
RUN pip install requests==2.32.3
RUN pip install -e git+https://github.com/resource-watch/cartosql.py.git#egg=cartosql
RUN pip install carto==1.11.2
RUN pip install LMIPy==0.6.2
RUN pip install cartoframes==1.2.5

# set name
ARG NAME=nrt-script
ENV NAME=${NAME}

# copy the application folder inside the container
RUN mkdir -p /opt/$NAME/data
WORKDIR /opt/$NAME/
COPY contents .

RUN useradd $NAME
RUN chown -R $NAME:$NAME /opt/$NAME
VOLUME /opt/$NAME/data
USER $NAME
ENV PYTHONPATH=/src

CMD ["python", "main.py"]