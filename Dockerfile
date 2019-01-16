FROM golang AS build-env
WORKDIR /gopath/app
ENV GOPATH /gopath/app
RUN mkdir /gopath/app/src
ADD . /gopath/app/src/
RUN cd /gopath/app/src && go build main.go

FROM daocloud.io/daocloud/go-busybox:glibc
COPY --from=build-env /gopath/app/ /
ENV TZ Asia/Shanghai
EXPOSE 9005
CMD ["/main"]