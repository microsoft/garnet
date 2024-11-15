---
id: code-structure
sidebar_label: Code Structure
title: Code Structure
---

# Garnet Code Structure

In the following list, we describe the C\# projects part of the Garnet Visual Studio solution as well as the folder containing them:
* **\\.azure\pipelines** folder
    * **Description:** Contains .yml files and other resources for running CI/CD pipelines in Azure DevOps
* **\\.github\workflows** folder
    * **Description:** Contains .yml files and other resources for running CI/CD pipelines in GitHub Actions
* **\benchmark** folder
  * BDN.benchmark
    * **Description:** A benchmarking tool using the [BenchmarkDotNet](https://github.com/dotnet/BenchmarkDotNet) library. This is currently the preferred tool for benchmarking different Garnet functionalities. Use command-line [arguments](https://github.com/dotnet/BenchmarkDotNet/blob/master/docs/articles/guides/console-args.md) to configure your run.
  * Resp.benchmark
    * **Description:** A benchmark tool to issue RESP commands to Garnet Server. Refer to the Resp benchmark page for details [here](../benchmarking/resp-bench.md).
* **\charts\garnet** folder
  * **Description:** Contains a Helm chart for Garnet.
* **\hosting\Windows** folder
  * Garnet.worker
    * **Description:** An app to host Garnet as a Windows Service.
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
    * Garnet.resources project
        * **Description:** Contains large text resources that can be dynamically loaded as needed, as to not overload the Garnet binaries.
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
    * CommandInfoUpdater project
        * **Description:** A tool for updating RESP commands metadata (info, docs) using a local RESP server (for usage instructions see: [Adding command info](garnet-api.md#adding-command-info)).
    * Embedded.perftest project
        * **Description:** A tool to issue RESP commands to Garnet server, while bypassing the network stack.
    * GarnetClientStress project
        * **Description:** A project to run stress tests against the GarnetClient library.    
    * GarnetJSON project
        * **Description:** A library defining a sample Garnet module that is capable of performing operations on JSON strings.
    * MigrateBench project
        * **Description:** A tool designed to test the performance and robustness of the migration operation by issuing batch migration requests between several nodes.
    * SampleModule project
        * **Description:** A library defining a sample Garnet module that is capable of performing custom commands, transactions and procedures.
    * TstRunner project
        * **Description:** Console application to run keys migration commands.
* **\samples** folder
    * GarnetClientSample project
        * **Description:** Example of how to use GarnetClient.
    * MetricsMonitor project
        * **Description:** Console app to get Latency metrics using StackExchange.Redis.
* **\test** folder
    * Garnet.test project
        * **Description:** Project for Garnet server unit tests, including unit tests of RESP commands.
    * Garnet.test.cluster project
        * **Description:** Project for Cluster unit tests.
    * testcerts folder
        * **Description:** Self-signed certificates for testing purposes when TLS is enabled. 
* **\website** folder
    * **Description:** Files for the Garnet documentation website.
* **root** folder        
    * **Description:** Editorconfig file, docker files, etc.
