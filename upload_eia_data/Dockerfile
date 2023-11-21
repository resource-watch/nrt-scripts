FROM python:3.9
MAINTAINER Weiqi Zhou <weiqi.zhou@wri.org>
# Note this script was originally developed by  Weiqi Zhou <weiqi.zhou@wri.org>

# install core libraries
RUN apt-get update && pip install -U pip

# install application libraries
RUN pip install pandas==2.0.3
RUN pip install boto3==1.10.45
RUN pip install requests==2.22.0
RUN pip install -e git+https://github.com/resource-watch/cartosql.py.git#egg=cartosql
RUN pip install carto==1.11.1
RUN pip install LMIPy==0.2.8
RUN pip install cartoframes==1.2.5
RUN pip install markupsafe==2.0.1

# set name
ARG NAME=nrt-script
ENV NAME ${NAME}

# copy the application folder inside the container
RUN mkdir -p /opt/$NAME/data
WORKDIR /opt/$NAME/
COPY contents .

RUN useradd -r $NAME
RUN chown -R $NAME:$NAME /opt/$NAME
VOLUME /opt/$NAME/data
USER $NAME
ENV PYTHONPATH /src

CMD ["python", "main.py"]