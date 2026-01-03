FROM nginx:1.25-alpine

# Copy NGINX configuration
COPY nginx.conf /etc/nginx/nginx.conf

# Create log directory
RUN mkdir -p /var/log/nginx

# Expose ports for backend services
# Note: These are internal only, not published to host
EXPOSE 8001 8002 8003 8004 50051 8090

CMD ["nginx", "-g", "daemon off;"]
