FROM nginx:1.25-alpine

# Install OpenSSL (for SSL support)
RUN apk add --no-cache openssl

# Copy custom nginx configuration
COPY nginx.conf /etc/nginx/nginx.conf

# Create directories
RUN mkdir -p /var/log/nginx /etc/nginx/ssl

# Note: SSL certificates are mounted from host at runtime via volume mount
# They should be located in ./certs/ directory on the host
# Generate them with: ./generate-ssl-cert.sh

# Copy fallback certificate generation script (in case certs not mounted)
COPY generate-cert-fallback.sh /docker-entrypoint.d/99-generate-cert-fallback.sh
RUN chmod +x /docker-entrypoint.d/99-generate-cert-fallback.sh

# Expose HTTP and HTTPS ports
EXPOSE 80 443

# Health check (try HTTPS first, fallback to HTTP)
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD wget --quiet --tries=1 --spider --no-check-certificate https://localhost/health || \
        wget --quiet --tries=1 --spider http://localhost/health || exit 1

# Start nginx in foreground
CMD ["nginx", "-g", "daemon off;"]
