FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY go.mod .
COPY main.go .
RUN go build -o server .

FROM alpine:3.19
WORKDIR /app
COPY --from=builder /app/server .
COPY china-packing-checklist.html .
ENV PORT=8080
EXPOSE 8080
CMD ["./server"]
