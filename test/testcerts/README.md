# TLS Configuration

In order to use Garnet with TLS, you can either create your own certificates or use our test certificate files (located at `<root>`/test/testcerts), to start both the server and the client for all the connections with TLS encryption.

### Note:
The provided certificates are self-signed and, are not trusted by default.
Also, they may use outdated hash and cipher suites that may not be strong. 
For better security, purchase a certificate signed by a well-known certificate authority.
These certificates should be used ONLY on dev/test environments.

## Using GarnetServer with TLS

For using TLS, it is required a pfx certificate, either in Windows or Linux, that contains the private and public key.

In the case that you have these two keys and you need the pfx format use the following command with openssl:

openssl pkcs12 -inkey `<server-name>`.key -in `<server-name>`.crt -export -out server-cert.pfx

### Startig GarnetServer with TLS

Use the following parameters to pass the certificate file details and password:

GarnetServer --cert-file-name `<path-to-file>`/server-cert.pfx --password `<cert-password>`

### Note:
The repository contains a pfx file under the path `<root>`/test/testcerts. If needed, use the testcert.pfx with the password `placeholder`.

## Using a resp-compatible client with TLS:

To connect a resp client with Garnet, you need a server cert, a private key and a ca cert.

Example:
Provide the following parameters when starting the client:

``
    --cacert garnet-ca.crt
    --cert garnet-cert.crt 
    --key garnet.key
``

## Generate your own self-signed certificates

1. Install openssl library

    For Linux distributions:

    `sudo apt install openssl`

    For Windows systems:

    Pick the most suitable option from [OpenSSL wiki](https://wiki.openssl.org/index.php/Binaries)

2. Run the following commands:

    2.1 Create the root key

    `openssl ecparam -out <issuer-name>.key -name prime256v1 -genkey`

    2.2 Create the root certificate and self-sign it:

    Use the following command to generate the Certificate Signing Request (CSR).

    `openssl req -new -sha256 -key <issuer-name>.key -out <issuer-name>.csr`

    Note: When prompted, type the password for the root key, and the organizational information for the custom CA such as Country/Region, State, Org, OU, and the name of the issuer.

    Use the following command to generate the Root Certificate:

    `openssl x509 -req -sha256 -days 365 -in <issuer-name>.csr -signkey <issuer-name>.key -out <issuer-name>.crt`

    This create will be used to sign your server certificate.

    2.3 Create the certificate's key

    Note: This server name must be different from the issuer's name.

    `openssl ecparam -out <server-name>.key -name prime256v1 -genkey`

    2.4  Create the CSR (Certificate Signing Request)

    `openssl req -new -sha256 -key <server-name>.key -out <server-name>.csr`

    Note: When prompted, type the password for the root key, and the organizational information for the custom CA: Country/Region, State, Org, OU, and the fully qualified domain name. 
    This is the domain of the server and it should be different from the issuer.

    2.5 Generate the certificate with the CSR and the key and sign it with the CA's root key

    `openssl x509 -req -in <server-name>.csr -CA <issuer-name>.crt -CAkey <issuer-name>.key -CAcreateserial -out <server-name>.crt -days 365 -sha256`

    2.6 Verify the newly created certificate

    `openssl x509 -in <server-name>.crt -text -noout`

    This will show you the certificate with the information you entered and you will aslo see the Issuer data and the Subject (server name).

    2.7 Verify the files in your directory, and ensure you have the following files:

    `<issuer-name>`.crt

    `<issuer-name>`.key

    `<server-name>`.crt

    `<server-name>`.key

## Exporting certs separately with openssl

If you have a certificate in a pfx format, you can extract all the certs and keys using openssl tool:

* Key

    `openssl pkcs12 -in testcert.pfx -nocerts -nodes -out garnet.key`

* Certificate

    `openssl pkcs12 -in testcert.pfx -clcerts -nokeys -out garnet-cert.crt`

* CA cacert

    `openssl pkcs12 -in testcert.pfx -cacerts -nokeys -chain -out garnet-ca.crt`

## Privacy

[Microsoft Privacy Statement](https://go.microsoft.com/fwlink/?LinkId=521839)