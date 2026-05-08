FROM python:3.9
LABEL maintainer="Weiqi Zhou <weiqi.zhou@wri.org>"
# Note this script was originally developed by Taufiq Rashid <taufiq.rashid@wri.org>

# install system libraries
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gdal-bin \
    libgdal-dev \
    && rm -rf /var/lib/apt/lists/*

# install application libraries
RUN pip install --no-cache-dir -U pip \
    && pip install --no-cache-dir \
        oauth2client==4.1.3 \
        google-api-python-client==2.135.0 \
        requests==2.32.3 \
        numpy==1.26.4 \
        Cython==3.0.10 \
    && pip install --no-cache-dir -e git+https://github.com/resource-watch/eeUtil#egg=eeUtil \
    && pip install --no-cache-dir GDAL=="$(gdal-config --version)" \
    && python -c "from osgeo import gdal, gdal_array; print(gdal.VersionInfo())"

# set name
ARG NAME=nrt-script
ENV NAME=${NAME}

# copy the application folder inside the container
RUN mkdir -p /opt/$NAME/data
WORKDIR /opt/$NAME/
COPY contents/ .

RUN useradd -r $NAME
RUN chown -R $NAME:$NAME /opt/$NAME
USER $NAME

CMD ["python", "main.py"]
