FROM --platform=$TARGETPLATFORM  openjdk:16-jdk-alpine3.12
RUN mkdir /etc/baetyl -p && mkdir /var/lib/baetyl/code -p 
WORKDIR /bin
COPY build.tar.gz .
RUN tar -zxvf build.tar.gz && rm build.tar.gz
CMD ["./build/install/examples/bin/java-runtime-server"]
