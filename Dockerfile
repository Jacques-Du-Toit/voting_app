# --- Stage 1: The Builder ---
# Use the official Rust image to compile the code
FROM rust:latest AS builder

# Create a working directory
WORKDIR /usr/src/voting_app

# Copy all files into the builder container
COPY . .

# Compile the app in release mode
RUN cargo build --release


# --- Stage 2: The Runner ---
# Use a stripped-down Linux image to save memory and start instantly
FROM debian:bookworm-slim

WORKDIR /app

# Copy ONLY the finished, compiled app from the builder stage
COPY --from=builder /usr/src/voting_app/target/release/voting_app .

# Copy frontend files so Axum can serve HTML/JS
COPY --from=builder /usr/src/voting_app/public ./public

# Tell the server we will be using port 3000 (though Koyeb overrides this dynamically)
EXPOSE 3000

# Start the server
CMD ["./voting_app"]