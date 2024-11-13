FROM paxos-builder as builder
COPY . /app
WORKDIR /app/cmd/client
RUN GOPROXY="https://goproxy.io" CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -o cli .
RUN mv cli ..
WORKDIR /app/cmd/server
RUN GOPROXY="https://goproxy.io" CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -o svr .
RUN mv svr ..


FROM alpine:3.16.3
# FROM scratch
WORKDIR /cmd
COPY --from=builder /app/cmd/* /cmd/

CMD [ "/cmd/svr" ]