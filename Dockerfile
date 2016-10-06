FROM ubuntu:latest
RUN apt-get -y update && apt-get -y install ffmpeg libcurl3 libpcrecpp0v5 && rm -rf /var/lib/apt/lists/*
COPY ./mp4split_1.7.19_ubuntu16_amd64.deb /tmp/mp4split_1.7.19_ubuntu16_amd64.deb
RUN dpkg -i /tmp/mp4split_1.7.19_ubuntu16_amd64.deb
COPY ./pfscheduler /go/app/
COPY entrypoint.sh /
CMD ["./entrypoint.sh"]
