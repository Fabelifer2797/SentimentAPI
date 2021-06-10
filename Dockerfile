FROM golang:1.16
RUN  mkdir /SENTIMENTAPI
WORKDIR /SENTIMENTAPI
COPY go.mod .
COPY go.sum .
RUN go mod download
COPY . . 
RUN  go build -o main .
CMD ["/SENTIMENTAPI/main"]