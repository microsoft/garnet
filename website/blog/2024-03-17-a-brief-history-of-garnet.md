---
slug: brief-history
title: A Brief History of Garnet
authors: badrishc
tags: [garnet, history, introduction]
---

Hi everyone! I just wanted to start off this blog with a short history of Garnet and how it
came to exist. At Microsoft Research, we have been working on storage technology
for a while. In 2016, we started working on a new key-value store design based on epoch protection and
a powerful storage API. This project, called [FASTER](https://github.com/microsoft/FASTER), was 
open-sourced in 2018 and gained a lot of traction within Microsoft and in the larger community. FASTER
has over 6k stars and over half a million NuGet downloads. Over the next several years, we built 
follow-on capabilities such as recoverability (CPR) and serverless support (Netherite), and the 
project was widely adopted.

Around 2021, we noticed a huge interest in remote cache-stores, particularly using APIs
such as the RESP API of Redis. Developers loved the flexibility of both the API and the
deployment model as a separate process/service. The cost savings compared to accessing 
cloud databases directly drove the adoption of caching layers, and these soon
grew to take up a significant portion of the operating cost of large services. When the
pandemic hit and online service usage spiked, there was a strong need for lowering costs
and improving performance (throughput and latency) for such caching layers.

We took on the challenge of building a new system, called Garnet, which could provide
extremely high performance end-to-end in a client-server setup while allowing clients
to remain largely unmodified by adopting RESP, the most popular wire protocols out there.
After a lot of design effort, we came up with a server threading model that could indeed 
make a huge end-to-end difference, often by orders-of-magnitude, in performance for 
basic get and set operations, with unmodified client code. This gave us the confidence to 
build out Garnet's feature set towards use in real scenarios.

The next question was API coverage. The Redis API is vast, and we were just a small research
team. Thankfully, our stack was built on .NET, which made tremendous advances in both performance
and richness of libraries. We designed a generic yet powerful way to define and use custom
data structures, and were able to quickly implement complex datatypes such as Sorted Set, List,
and Hash by reusing data structures in C#. We then built complex structures such as HyperLogLog
and Bitmap as well, and added transactions and extensibility features.

The next requirement was scale-out and recovery, for which we designed write-ahead operation logging,
sharding capability, replication, and key migration for dynamic scale-out. By keeping basic compatibility
with the Redis API, we were able to add these features in a way that existing client code could
be largely unchanged.

After thousands of unit tests and a couple of years working with first-party teams at Microsoft
deploying Garnet in production (more on this in future blog posts!), we felt it was time to release 
it publicly, just like we did with FASTER five years back. We wanted developers across the planet to 
benefit from this powerful technology and contribute back to the codebase as well. We felt that 
the modern C# codebase would be particularly attractive here, in terms of ease of expansion, 
maintenance, and contribution. Coming from MSR, we also wanted people in academia to conduct research,
collaborate with us, and expand various aspects of the system.

So, explore Garnet, see if you can find use for it in your applications, consider helping us out 
with expanding its capabilities, and as always, let us know what you think!
