FROM golang:1.18

# Set the working directory inside the container
WORKDIR /app

# Copy the go.mod and go.sum files first and download the dependencies.
# This is done before copying the rest of the code to leverage Docker cache,
# allowing for faster subsequent builds if your dependencies don't change.
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the source code
COPY . .

# Compile the application. Replace 'main.go' and 'app' with the appropriate file names.
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o app main.go

# Use a smaller, final base image
FROM alpine:latest

# Install ca-certificates in case your app makes external HTTPS requests
RUN apk --no-cache add ca-certificates

WORKDIR /root/

# Copy the compiled application from the first stage.
COPY --from=0 /app/app .

# Define a volume for persisting state
VOLUME /root/state

# Expose the port your application listens on
EXPOSE 8080

# Command to run the application
CMD ["./app"]
