FROM python:3.9
LABEL maintainer="Kristine Lister <kristine.lister@wri.org>"
#Note this script was originally developed by Ken Wakabayashi <ken.wakabayashi@wri.org> and Kristine Lister <kristine.lister@wri.org>

# install core libraries
RUN apt-get update
RUN pip install -U pip

# install application libraries
RUN pip install --upgrade pip
RUN pip install requests==2.32.0
RUN pip install -e git+https://github.com/resource-watch/cartosql.py.git#egg=cartosql
RUN pip install numpy==1.26.4
RUN pip install pandas==2.2.2
RUN pip install boto3==1.11.8
RUN pip install shapely==1.6.4
RUN pip install carto==1.8.4 cartoframes==0.9.2


# set name
ARG NAME=nrt-script
ENV NAME=${NAME}

# copy the application folder inside the container
RUN mkdir -p /opt/$NAME/data
WORKDIR /opt/$NAME/
COPY contents .

RUN useradd -r $NAME
RUN chown -R $NAME:$NAME /opt/$NAME
#USER $NAME

CMD ["python", "main.py"]
