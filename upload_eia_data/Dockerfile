FROM python:3.6
MAINTAINER Weiqi Zhou <weiqi.zhou@wri.org>

# install core libraries
RUN apt-get update && pip install -U pip

# install application libraries
RUN pip install pandas==0.24.2
RUN pip install boto3==1.10.45
RUN pip install requests==2.22.0
RUN pip install -e git+https://github.com/resource-watch/cartosql.py.git#egg=cartosql
RUN pip install carto==1.11.1
RUN pip install LMIPy==0.2.8
RUN pip install cartoframes==1.0.2

# set name
ARG NAME=upload-world-bank
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