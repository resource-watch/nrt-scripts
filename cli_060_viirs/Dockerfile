FROM python:3.9
MAINTAINER Amelia Snyder <amelia.snyder@wri.org>
#Note this script was originally developed by Francis Gassert <fgassert@wri.org>

# install core libraries
RUN apt-get update
RUN pip install -U pip

# install application libraries
RUN pip install requests

# set name
ARG NAME=nrt-script
ENV NAME ${NAME}

# copy the application folder inside the container
WORKDIR /opt/$NAME/
COPY contents/ .

RUN useradd $NAME
RUN chown $NAME:$NAME .
USER $NAME

CMD ["python", "main.py"]
