FROM ubuntu:latest
RUN apt-get -y update && apt-get -y install ffmpeg libcurl3 libpcrecpp0v5 golang git wget && rm -rf /var/lib/apt/lists/*
RUN wget -O /tmp/mp4split_1.7.19_ubuntu16_amd64.deb http://sources.afrostream.net/packages/mp4split_1.7.19_ubuntu16_amd64.deb
RUN dpkg -i /tmp/mp4split_1.7.19_ubuntu16_amd64.deb
COPY ./src/* /go/src/pfscheduler/
COPY ./compile-pfscheduler.sh /tmp/
RUN /tmp/compile-pfscheduler.sh
COPY ./usp_package_sub.sh /usr/local/bin/
COPY ./entrypoint.sh /
CMD ["./entrypoint.sh"]
