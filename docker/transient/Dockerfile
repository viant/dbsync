FROM ubuntu:16.04

RUN apt-get update
RUN apt-get install -y wget build-essential gcc libc-bin libaio1 pkg-config git pkg-config

WORKDIR /

COPY dep.tar.gz .
RUN tar xvzf dep.tar.gz && \
    rm dep.tar.gz

RUN wget http://www.unixodbc.org/unixODBC-2.3.5.tar.gz &&\
    tar xzvf unixODBC-2.3.5.tar.gz &&\
    cd /usr/local/lib/ && \
    ln -s libodbc.so.2.0.0 libodbc.so.1 && \
    ln -s libodbcinst.so.2.0.0 libodbcinst.so.1 && \
    cd - &&\
    cd unixODBC-2.3.5 &&\
    ./configure --sysconfdir=/etc --disable-gui --disable-drivers --enable-iconv --with-iconv-char-enc=UTF8 --with-iconv-ucode-enc=UTF16LE &&\
    make &&\
    make install &&\
    cd .. && \
    rm -rf unixODBC-2.3.5 unixODBC-2.3.5.tar.gz

RUN wget https://dl.google.com/go/go1.16.1.linux-amd64.tar.gz && \
    tar -C /usr/local -xzf go1.16.1.linux-amd64.tar.gz && \
    rm go1.16.1.linux-amd64.tar.gz && \
    ln -s /usr/local/go/bin/go /usr/local/bin/go


ENV PKG_CONFIG_PATH=/
ENV LD_LIBRARY_PATH=/usr/local/lib:/usr/lib/oracle/12.2/client64/lib

COPY . /tmp/dbsync

WORKDIR /tmp/dbsync/sync/app
RUN go mod tidy
RUN go build -ldflags "-X main.Version=${Version}" -o dbsync &&  cp dbsync /dbsync
RUN tar cvzf /dbsync.tar.gz /dbsync  \
    /usr/local/lib/l* /etc/vertica.ini /etc/odbcinst.ini /opt/vertica \
    /usr/lib/oracle /etc/environment

WORKDIR /tmp/dbsync/transfer/app
RUN go build -ldflags "-X main.Version=${Version}" -o dbtransfer &&  cp dbtransfer /dbtransfer
RUN tar cvzf /dbtransfer.tar.gz /dbtransfer  \
    /usr/local/lib/l* /etc/vertica.ini /etc/odbcinst.ini /opt/vertica \
    /usr/lib/oracle /etc/environment

ENV VERTICAINI=/etc/vertica.ini
CMD ["/dbsync"]
