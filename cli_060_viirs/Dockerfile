FROM python:3.6-alpine
MAINTAINER Francis Gassert <fgassert@wri.org>

# install core libraries
RUN apk update

# install application libraries
RUN pip install requests

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
