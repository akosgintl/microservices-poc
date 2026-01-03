#!/bin/sh
# Fallback: Generate self-signed SSL certificate if not mounted from host
# This script runs as a Docker entrypoint if certificates don't exist

set -e

CERT_DIR="/etc/nginx/ssl"
CERT_FILE="${CERT_DIR}/nginx-fe.crt"
KEY_FILE="${CERT_DIR}/nginx-fe.key"

# Check if certificates already exist (mounted from host)
if [ -f "${CERT_FILE}" ] && [ -f "${KEY_FILE}" ]; then
    echo "✓ SSL certificates found (mounted from host)"
    echo "  Certificate: ${CERT_FILE}"
    echo "  Key: ${KEY_FILE}"
    exit 0
fi

# Certificates not found, generate them as fallback
echo "⚠ SSL certificates not found, generating fallback certificates..."
echo "  Recommended: Generate certificates on host with ./generate-ssl-cert.sh"
echo "  and mount them via docker-compose.yml"

# Create SSL directory if it doesn't exist
mkdir -p ${CERT_DIR}

# Generate self-signed certificate
# Valid for 365 days
openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
  -keyout ${KEY_FILE} \
  -out ${CERT_FILE} \
  -subj "/C=US/ST=State/L=City/O=MicroservicesPOC/OU=DevOps/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,DNS:nginx-fe,IP:127.0.0.1"

echo "✓ Fallback SSL certificate generated:"
echo "  Certificate: ${CERT_FILE}"
echo "  Key: ${KEY_FILE}"

# Set appropriate permissions
chmod 644 ${CERT_FILE}
chmod 600 ${KEY_FILE}

echo "✓ Permissions set"
echo "✓ Certificate is valid for 365 days"
echo ""
echo "Note: For production, generate certificates on host:"
echo "  ./generate-ssl-cert.sh"
echo "  docker compose up -d --build nginx-fe"
