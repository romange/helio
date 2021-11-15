### Generate private keys

pem is a textual format on how to encode binary data.

```bash
openssl genrsa -out rsa_key.pem 2048    # Or
openssl ecparam -name secp521r1 -genkey -noout -out ec_key.pem  # ecdsa

```

To inspect use: `openssl pkey -in rsa_key.pem -pubout`  which works for all formats.

### Generate CSR (Certificate Signing Request)

```bash
openssl req -new -sha256 -key rsa_key.pem  -subj "/O=TestOrg/CN=localhost" -out my.csr

openssl req -in my.csr -noout -text -verify # To inspect & verify
openssl req -in my.csr -noout -pubkey  # To show public key
```

To combine both: `openssl req -newkey rsa:2048 -nodes -keyout rsa_key.key -out domain.csr -subj "..."`

### Generate certificate

```bash
openssl x509  -signkey rsa_key.pem -in my.csr  -req -days 365 -out domain.crt
openssl x509 -text -noout -in domain.crt   # To inspect
openssl x509 -noout -in domain.crt -pubkey  # To show public key
```


### Generate certificate with ESDCA
One liner:

```bash
openssl req -new  -newkey ec -pkeyopt ec_paramgen_curve:secp256k1 -keyout ec_key.pem -x509 \
-nodes -days 1000 -out cert.pem -subj "/O=UnitTest/CN=localhost"
```

## Applications

For CA we can create private key/crt:

```
openssl genrsa -out ca.key 4096
openssl req -x509 -new -nodes -sha256 -key ca.key -days 3650 \
    -subj '/O=Test/CN=Certificate Authority' -out ca.crt
```
Please note that CA's CN should be different than of server certificate.

For CA-signed certificates we provide CA key/certificates created earlier:

```
openssl genrsa -out mykey.pem 2048

openssl req -new -sha256 -subj "/O=Test/CN=myserver.domain" \
        -key mykey.pem | \
    openssl x509 -req -sha256 -CA ca.crt -CAkey ca.key -CAserial ca.txt \
     -CAcreateserial -days 365 -out mycert.pem
```