FROM python:3.6-alpine
MAINTAINER Amelia Snyder <amelia.snyder@wri.org>
ARG NAME=soc_018
ENV NAME ${NAME}

# install core libraries
RUN apk update
RUN apk add git

# install application libraries
RUN pip install --upgrade pip && pip install requests==2.22.0
RUN pip install -e git+https://github.com/resource-watch/cartosql.py.git#egg=cartosql

# Copy the application folder inside the container
RUN mkdir -p /opt/$NAME/data
WORKDIR /opt/$NAME/
COPY contents/ .

# Set user for container security
RUN addgroup -S $NAME && adduser -S -G $NAME $NAME
RUN chown $NAME:$NAME /opt/$NAME
USER $NAME

# Launch script
CMD ["python", "main.py"]
