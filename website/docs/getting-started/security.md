---
id: security
sidebar_label: Security
title: Securing your Garnet Deployment
slug: /security
---

Garnet supports TLS/SSL connections using the SslStream capability built into .NET. You can enable end to end encryption using this functionality, discussed next.
Garnet also supports basic password-based authentication on the wire, following the AUTH command in RESP, this is discussed at the end of the section.

## TLS Configuration

In order to use Garnet with TLS, you can create your own certificates. For strictly test purposes, we include test certificate files (located 
at `<root>/test/testcerts`). You need to start both the server and the client with TLS encryption.

## Using GarnetServer with TLS

On the server side, you need to start Garnet with TLS enabled. From the command prompt, the parameters to add are `--tls` to enable TLS, details of the certificates such as certificate 
name (we only accept .pfx files) via `--cert-file-name`, TLS certificate password via `--cert-password`, whether TLS client certificate is required via `--client-certificate-required`, issuer 
certificate to validate against via `--issuer-certificate-path`, and whether TLS checks certificate revocation via `--certificate-revocation-check-mode`. You can also use a certificate via
subject name on Windows via `cert-subject-name`. Certificate refresh can be done automatically via the option `--cert-refresh-freq`.

```bash
    GarnetServer --tls --cert-file-name testcert.pfx --cert-password placeholder
```

If you host your own GarnetServer via NuGet, you can specify the SSL connection options directly by passing in an instance of your implementation of `IGarnetTlsOptions`, we have a 
prototype sample of this as `GarnetTlsOptions.cs`.

:::tip
In case you have the private and public key files in .key and .crt formats, you can create the .pfx format using openssl:

```bash
    openssl pkcs12 -inkey <server-name>.key -in <server-name>.crt -export -out server-cert.pfx
```
:::

**Note:**

The repository contains a test .pfx file under the path `<root>/test/testcerts`. For testing, use can try testcert.pfx file with the password `placeholder`. The sample test certificates
in Garnet repository are self-signed and, are not trusted by default. Also, they may use outdated hash and cipher suites that may not be strong. For better security, purchase a 
certificate signed by a well-known certificate authority. These certificates may be used ONLY on dev/test environments.

## Using a RESP-compatible client with TLS

To connect a RESP client with Garnet, you need a server certificate, a private key, and a CA certificate file. For example, provide the following parameters when starting a client:

```bash
    --cacert garnet-ca.crt
    --cert garnet-cert.crt 
    --key garnet.key
```

## Generating your own self-signed certificates

Follow these steps, if you need to generate a self-signed certificate for testing TLS with Garnet Server.

1. Install openssl library

    For Linux distributions:

    ```bash
    sudo apt install openssl
    ```
    
    For Windows systems:

    Pick the most suitable option from [OpenSSL wiki](https://wiki.openssl.org/index.php/Binaries)

2. Run the following commands:

    2.1 Create the root key

    ```bash
    openssl ecparam -out <issuer-name>.key -name prime256v1 -genkey
    ```
    
    2.2 Create the root certificate and self-sign it:

    Use the following command to generate the Certificate Signing Request (CSR).

    ```bash
    openssl req -new -sha256 -key <issuer-name>.key -out <issuer-name>.csr
    ```

    Note: When prompted, type the password for the root key, and the organizational information for the custom CA such as Country/Region, State, Org, OU, and the name of the issuer.

    Use the following command to generate the Root Certificate:

    ```bash 
    openssl x509 -req -sha256 -days 365 -in <issuer-name>.csr -signkey <issuer-name>.key -out <issuer-name>.crt
    ```

    This create will be used to sign your server certificate.

    2.3 Create the certificate's key

    Note: This server name must be different from the issuer's name.

    ```bash
    openssl ecparam -out <server-name>.key -name prime256v1 -genkey
    ```

    2.4  Create the CSR (Certificate Signing Request)

    ```bash 
    openssl req -new -sha256 -key <server-name>.key -out <server-name>.csr
    ```

    Note: When prompted, type the password for the root key, and the organizational information for the custom CA: Country/Region, State, Org, OU, and the fully qualified domain name. 
    
    This is the domain of the server and it should be different from the issuer.

    2.5 Generate the certificate with the CSR and the key and sign it with the CA's root key

    ```bash 
    openssl x509 -req -in <server-name>.csr -CA <issuer-name>.crt -CAkey <issuer-name>.key -CAcreateserial -out <server-name>.crt -days 365 -sha256
    ```

    2.6 Verify the newly created certificate

    ```bash
    openssl x509 -in <server-name>.crt -text -noout
    ```

    This will show you the certificate with the information you entered and you will aslo see the Issuer data and the Subject (server name).

    2.7 Verify the files in your directory, and ensure you have the following files:

    `<issuer-name>`.crt

    `<issuer-name>`.key

    `<server-name>`.crt

    `<server-name>`.key

## Exporting certs separately with openssl

If you have a certificate in a pfx format, you can extract all the certs and keys using openssl tool:

* Key

    ```bash
    openssl pkcs12 -in testcert.pfx -nocerts -nodes -out garnet.key
    ```

* Certificate

    ```bash
    openssl pkcs12 -in testcert.pfx -clcerts -nokeys -out garnet-cert.crt
    ```

* CA cacert

    ```bash 
    openssl pkcs12 -in testcert.pfx -cacerts -nokeys -chain -out garnet-ca.crt 
    ```

## Password Protected Sessions

Garnet support password protected sessions, using the AUTH mechanism of the RESP protocol. When you create a Garnet server, you can specify enabling authentication via the flag --auth and the type of authentication you want to enable. The following are the options to enable or disable password protected connections:

* NoAuth, by default there is no password requirement.

```bash
    GarnetServer --auth NoAuth
```

* Password, indicates to the server that clients should use **auth** command and a password, before sending requests.

Start the server including the password:

```bash
    GarnetServer --auth Password --password <passwordplaceholder>
```

With these two features, you can get basic security capabilities with your Garnet deployment. Please check with your team's security requirements whether this functionality is sufficient to match your 
needs. Note that if larger-than-memory data is enabled, we do not encrypt the data that spills to local disk, or the checkpoints. We also do not encrypt the actual data served in memory.
