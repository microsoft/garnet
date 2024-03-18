---
id: code-structure
sidebar_label: Code Structure
title: Code Structure
---

# Garnet Code Structure

In the following list, we describe the C\# projects part of the Garnet Visual Studio solution as well as the folder containing them:
* **\benchmark** folder
  * Resp.benchmark
    * **Description:** A benchmark tool to issue RESP commands to Garnet Server. Refer to the Resp benchmark page for details [here](../benchmarking/resp-bench.md).
* **\libs** folder
    * storage\Tsavorite folder
        * **Description:** Solution, sources, and tests for Tsavorite library.
    * Garnet.client project
        * **Description:** Clients that can be used with Garnet server with limited functionality.
     * Garnet.cluster project
        * **Description:** The Garnet Cluster implementation.
    * Garnet.common project
        * **Description:** Network layer implementation and common tools used for requests and responses processing.
    * Garnet.host project
        * **Description:** The hosting class that instantiates the Garnet server.
    * Garnet.server project
        * **Description:** All the API for the keys operations, ACL commands, Checkpointing, Metrics, Transactions, Garnet server API, and storage layer implementation with RESP.   
* **\main** folder
    * GarnetServer project
        * **Description:** The main program (entry point) where the Garnet server executable is created using the host.
* **\metrics** folder
    * HdrHistogram project
        * **Description:** Project for creating High Dynamic Range (HDR) Histograms used for graphs of different server metrics.
* **\playground** folder
    * Bitmap project
        * **Description:** A project that exercises different bitmaps operations, using Tsavorite.
    * ClusterStress project
        * **Description:** A work in progress project that executes stress tests over a Garnet server with cluster mode. 
    * Embedded.perftest project
        * **Description:** A tool to issue RESP commands to Garnet server, while bypassing the network stack.
    * GarnetClientStress project
        * **Description:** A project to run stress tests against the GarnetClient library.    
    * TstRunner project
        * **Description:** Console application to run keys migration commands.
* **\samples** folder
    * GarnetClientSample project
        * **Description:** Example of how to use GarnetClient.
    * MetricsMonitor project
        * **Description:** Console app to get Latency metrics using StackExchange.Redis.
* **root** folder        
    * **Description:** Editorconfig file, docker files, etc.
* **\test** folder
    * Garnet.test project
        * **Description:** Project for Garnet server unit tests, including unit tests of RESP commands.
    * Garnet.test.cluster project
        * **Description:** Project for Cluster unit tests.
    * testcerts folder
        * **Description:** Self-signed certificates for testing purposes when TLS is enabled. 
* **\website** folder
    * **Description:** Files for the Garnet documentation website.
* **editorconfig** file
    * **Description:** Definition of Coding-style rules.


