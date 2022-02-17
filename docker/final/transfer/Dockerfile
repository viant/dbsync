
FROM ubuntu:16.04
RUN apt-get update -y && apt-get install -y  build-essential gcc libc-bin libaio1 git

WORKDIR /

COPY dbtransfer.tar.gz .

ENV VERTICAINI=/etc/vertica.ini
ENV PKG_CONFIG_PATH=/
ENV LD_LIBRARY_PATH=/usr/local/lib:/usr/lib/oracle/12.2/client64/lib
RUN tar xvzf /dbtransfer.tar.gz && rm dbtransfer.tar.gz

CMD ["/dbtransfer"]
