FROM python:3.6-alpine
MAINTAINER Amelia Snyder <amelia.snyder@wri.org>
#Note this script was originally developed by Francis Gassert <fgassert@wri.org>

# install core libraries
RUN apk update

# install application libraries
RUN pip install requests==2.22.0

# set name
ARG NAME=nrt-script
ENV NAME ${NAME}

# copy the application folder inside the container
WORKDIR /opt/$NAME/
COPY contents/ .

RUN adduser -D $NAME
RUN chown $NAME:$NAME .
USER $NAME

CMD ["python", "main.py"]
