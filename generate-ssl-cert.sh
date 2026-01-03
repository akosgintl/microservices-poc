#!/bin/bash
# Generate self-signed SSL certificates for nginx-fe
# These certificates can be mounted into the nginx-fe container

set -e

# Configuration
CERT_DIR="./certs"
CERT_FILE="${CERT_DIR}/nginx-fe.crt"
KEY_FILE="${CERT_DIR}/nginx-fe.key"
DAYS_VALID=365

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "=========================================="
echo "  SSL Certificate Generator for nginx-fe"
echo "=========================================="
echo ""

# Create certificate directory
echo "Creating certificate directory..."
mkdir -p ${CERT_DIR}

# Check if certificates already exist
if [ -f "${CERT_FILE}" ] && [ -f "${KEY_FILE}" ]; then
    echo -e "${YELLOW}Warning: Certificates already exist!${NC}"
    echo "  Certificate: ${CERT_FILE}"
    echo "  Key: ${KEY_FILE}"
    echo ""
    read -p "Do you want to overwrite them? (y/N) " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Aborted. Existing certificates were not modified."
        exit 0
    fi
    echo ""
fi

# Generate self-signed certificate
echo "Generating self-signed SSL certificate..."
echo "  Valid for: ${DAYS_VALID} days"
echo "  Subject: CN=localhost"
echo ""

# Prevent Git Bash on Windows from converting paths
export MSYS_NO_PATHCONV=1

openssl req -x509 -nodes -days ${DAYS_VALID} -newkey rsa:2048 \
  -keyout "${KEY_FILE}" \
  -out "${CERT_FILE}" \
  -subj "/C=US/ST=State/L=City/O=MicroservicesPOC/OU=Development/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,DNS:nginx-fe,DNS:*.localhost,IP:127.0.0.1,IP:0.0.0.0"

echo ""
echo -e "${GREEN}✓ SSL certificate generated successfully!${NC}"
echo ""
echo "Certificate details:"
echo "  Certificate: ${CERT_FILE}"
echo "  Private Key: ${KEY_FILE}"
echo "  Valid for: ${DAYS_VALID} days"
echo ""

# Set appropriate permissions
chmod 644 ${CERT_FILE}
chmod 600 ${KEY_FILE}

echo -e "${GREEN}✓ Permissions set${NC}"
echo "  Certificate: 644 (readable by all)"
echo "  Private Key: 600 (readable by owner only)"
echo ""

# Display certificate information
echo "Certificate Information:"
echo "----------------------------------------"
openssl x509 -in ${CERT_FILE} -noout -subject -issuer -dates -ext subjectAltName
echo "----------------------------------------"
echo ""

# Display fingerprint
echo "Certificate Fingerprint (SHA256):"
openssl x509 -in ${CERT_FILE} -noout -fingerprint -sha256
echo ""

# Calculate expiration
EXPIRY_DATE=$(openssl x509 -in ${CERT_FILE} -noout -enddate | cut -d= -f2)
echo -e "${YELLOW}Certificate expires on: ${EXPIRY_DATE}${NC}"
echo ""

echo "Next steps:"
echo "1. Start/restart nginx-fe to use the new certificates:"
echo "   docker compose up -d --build nginx-fe"
echo ""
echo "2. Access your application:"
echo "   https://localhost"
echo ""
echo "3. Browser will warn about self-signed certificate - this is expected."
echo "   Click 'Advanced' → 'Proceed to localhost (unsafe)'"
echo ""
echo -e "${GREEN}Done!${NC}"
