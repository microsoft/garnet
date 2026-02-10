---
id: onboarding
sidebar_label: Onboarding
title: Onboarding
---

# Onboarding for Garnet Development

Welcome to Garnet! In this page, you will find the main steps to set up your work environment for developing in Garnet.

## Quick start & useful resources

For an introduction to Garnet and its capabilities, you can start with [Welcome to Garnet](../welcome/intro.md) page.

**Additionally the following pages and documentation might be helpful:**

- [RESP specification](https://redis.io/docs/reference/protocol-spec/)
- [Redis data types](https://redis.io/docs/data-types/)
- [FASTER](https://microsoft.github.io/FASTER/docs/fasterkv-basics/), which we forked for use in Garnet as [Tsavorite](tsavorite/intro.md)

### Tools

- Visual Studio 2026 (Preview version recommended)
- .NET 10 SDK
- Git
- Azure Windows VM
- Azure Linux VM
- An interactive RESP client, such as
  - On Linux: redis-cli, interactive RESP client in Linux
  - On Windows: Memurai (for using memurai-cli client) or redis-cli via WSL
  - RedisInsight for experimenting with contents of the store
  - telnet

### Start hacking

1. Clone the repository

```bash
git clone https://github.com/microsoft/garnet.git
```

After cloning the repository you can either run the unit tests or run the server and use one of the RESP client suggested in Windows or Linux.

2. Run the tests suite

```bash
dotnet test -c Release -f net10.0 -l "console;verbosity=detailed"
```

3. Run the server

Using a size memory of 4 GB and index size of 64 MB:

```bash
cd <root>/main/GarnetServer/
dotnet run -c Debug -f net10.0 -- --logger-level Trace -m 4g -i 64m
```

4. Use the Memurai client in Windows to send commands to Garnet. A guide about how to install Memurai on Windows can be found [here](https://docs.memurai.com/en/installation.html).

5. If you are using Linux, you can use the redis-cli tool. Our official supported Linux distribution is Ubuntu.

6. A third option is to install Redis-Insight on Windows. Follow the official guide [here](https://redis.com/redis-enterprise/redis-insight/#insight-form).

## Troubleshooting

1. If you need to use TLS in Linux, follow the guide at:

   `<root>/Garnet/test/testcerts/README.md`

2. If you need to run the local device library, make sure to have these dependencies:

   ```bash
   sudo apt install -y g++ libaio-dev uuid-dev libtbb-dev
   ```

## Garnet API development

### Code Patterns

All requests to the server are either basic RESP commands or array RESP commands. The following shows one example of each of them:

Basic Command: PING
RESP representation:

```
$4\r\nPING\r\n
```

Array Command: SET mykey abc

RESP representation:

```
*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nabcdefg\r\n
```

### Development concepts 

* Understanding of the types of memory in C#: Managed Heap, Stack memory, Unmanaged memory, etc.

* Use of Span and SpanByte

    **Tsavorite** and **Garnet** rely heavily on these two types for allocating data in memory and then transfer it on the network layer. Understanding and familiarity with both of them will be very helpful for a better understanding of the code in general.

    * [Documentation about Span](https://learn.microsoft.com/en-us/dotnet/api/system.span-1?view=net-7.0)
 
    * [Unsafe code best practices](https://learn.microsoft.com/en-us/dotnet/standard/unsafe-code/best-practices)

    * [Use of pointers and unsafe code](https://learn.microsoft.com/en-us/dotnet/csharp/language-reference/unsafe-code)

    * In the project:

        Implementation of SpanByte:
        
        `<root>`\Tsavorite\cs\src\core\VarLen\SpanByte.cs


## Pull Request protocol

Any new feature, change to existing functionality or bug fixing needs to be done using the following process:

1. Create an Issue Item on the [Garnet Project](https://github.com/microsoft/Garnet), use the following criteria: Enhancement for new features, Bug for fixes, Task for small improvements or changes.

2. Is a good practice to create your own local branch with the following naming convention: 

    `<username>`/branch-name

3. Include Unit Tests for any new commands or feature. Allure enabled tests are required.  

   Full documentation about Allure can be found [here](https://allurereport.org/docs/).  

   Each test class must:  
    * Apply [AllureNUnit] custom attribute  
    * Apply [TestFixture] attribute 
    * Inherit from the AllureTestBase base class 

4. Once it is ready for review, create a [Pull Request](https://github.com/microsoft/Garnet/pulls). Make sure to link it to your issue item in the development section.


## Formatting style guide

* Comments are an important part of the documentation. Make sure your code includes them.

* The official format for comments is:
 
    `//<one whitespace> Comment starting with a capital letter`

    Example:

    `// This comment has good formatting `

* Methods should have their summary block comment and description for each parameter. 

    Example:

```csharp

/// <summary>
/// Iterates the set of keys in the main store.
/// </summary>
/// <param name="patternB">The pattern to apply for filtering</param>
/// <param name="allKeys">When true the filter is omitted</param>
/// <param name="cursor">The value of the cursor in the command request</param>
/// <param name="storeCursor">Value of the cursor returned</param>
/// <param name="Keys">The list of keys from the stores</param>
/// <param name="count">The size of the batch of keys</param>
/// <param name="type">Type of key to filter out</param>
/// <returns></returns>
public bool DbScan(ArgSlice patternB, bool allKeys, long cursor, out long storeCursor, out List<byte[]> Keys,  long count = 10, Span<byte> type = default);
```

* As a good practice, follow the <b>camel case C# naming convention.</b>

## Garnet project structure

* Refer to the Code Structure page for details [here](../dev/code-structure.md).

## Running the benchmark application

* Refer to the Resp benchmark page for details [here](../benchmarking/resp-bench.md).

## Build

* For details refer to the [Build and Test page](../getting-started/build.md)

## Test

As a sanity check, you can run the Garnet test suite. The command to run tests in Release mode for .NET 10 is shown below (make sure you are in the root folder of the repo).

``` bash
dotnet test -c Release -f net10.0 -l "console;verbosity=detailed"
```

Note that Tsavorite has its own solution file and test suite in the folder `<root>`/Garnet/libs/storage/Tsavorite.

**Tip:** By default, Garnet listens to TCP port 6379, you can use this information to adjust your firewall settings if you need to access Garnet from remote machines.



