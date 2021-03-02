FROM golang as builder

# Set the Current Working Directory inside the container
WORKDIR /app

# Copy the source from the current directory to the Working Directory inside the container
COPY . .

# Build the Go app
RUN make build

######## Start a new stage from scratch #####
FROM alpine

RUN apk --no-cache add ca-certificates

# Create a group and user
RUN addgroup -S appgroup -g 1000 && adduser -S appuser -u 1000 -G appgroup

WORKDIR /app

RUN chown -R appuser.appgroup /app

# Tell docker that all future commands should run as the appuser user
USER appuser

# Copy the Pre-built binary file from the previous stage
COPY --from=builder /app/bin/srv_linux app

# Command to run the executable
ENTRYPOINT "./app"
