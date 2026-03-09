# Helio TLS Certificates for Unit Tests

**⚠️ FOR TESTING PURPOSES ONLY ⚠️**

This directory contains **static, long-lived TLS certificates** specifically intended for C++ unit tests (e.g., `tls_socket_test.cc`) and Continuous Integration (CI) environments.

Unlike development certificates which are often generated on-the-fly and expire quickly, these files:
1.  **Are Checked In:** They are part of the repository to ensure tests run deterministically on every machine.
2.  **Have Long Expiry:** Valid for 100 years to prevent random CI failures in the future.
3.  **Are Insecure:** The private keys are public knowledge. **Never use these in production.**

## File Overview

| File | Role | Description |
| :--- | :--- | :--- |
| `ca-cert.pem` | **Trust Root** | The CA certificate. All servers and clients in unit tests must trust this. |
| `ca-key.pem` | **CA Key** | Private key used to sign the server/client certs. |
| `server-cert.pem` | **Server Certificate** | Public cert presented by the server during the TLS handshake. |
| `server-key.pem` | **Server Private Key** | Private key for the server. Never use outside unit tests. |
| `client-cert.pem` | **Client Certificate** | Public cert used for mTLS client authentication. |
| `client-key.pem` | **Client Private Key** | Private key for the client. Never use outside unit tests. |

## How to Regenerate

If these certificates need to be replaced, run the following commands in this directory:

```bash
# 1. Generate Certificate Authority (CA)
openssl req -new -x509 -days 36500 -nodes -subj "/CN=TestCA" \
    -keyout ca-key.pem -out ca-cert.pem

# 2. Generate Server Cert
openssl req -new -newkey rsa:2048 -nodes -subj "/CN=localhost" \
    -keyout server-key.pem -out server.csr
openssl x509 -req -in server.csr -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial \
    -days 36500 -out server-cert.pem

# 3. Generate Client Cert
openssl req -new -newkey rsa:2048 -nodes -subj "/CN=TestClient" \
    -keyout client-key.pem -out client.csr
openssl x509 -req -in client.csr -CA ca-cert.pem -CAkey ca-key.pem \
    -days 36500 -out client-cert.pem

# 4. Cleanup intermediate files
rm *.csr *.srl