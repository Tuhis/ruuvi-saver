# Start from the latest golang base image
FROM golang:1.25.4 AS builder

# Set the Current Working Directory inside the container
WORKDIR /app

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download all dependencies. Dependencies will be cached if the go.mod and go.sum files are not changed
RUN go mod download

# Copy the source from the current directory to the Working Directory inside the container
COPY . .

# Build the Go app
RUN CGO_ENABLED=0 GOOS=linux make build

# Start a new stage from scratch
FROM gcr.io/distroless/static:nonroot

# Use distroless as minimal base image to package the app binary
# Refer to https://github.com/GoogleContainerTools/distroless for more details
# And to https://hackernoon.com/distroless-containers-hype-or-true-value-2rfl3wat for a good explanation
COPY --from=builder /app/ruuvi-saver /ruuvi-saver

# Command to run the executable
CMD ["/ruuvi-saver"]
