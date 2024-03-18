---
id: faq
sidebar_label: FAQ
title: Frequently Asked Questions (FAQ)
---

### How can I contribute to this documentation website?

This website is built using [Docusaurus 2](https://docusaurus.io/), a modern static website generator. The sources are
on GitHub, linked [here](https://github.com/microsoft/garnet/edit/main/website). The markdown files for the documentation 
are in the [docs](https://github.com/microsoft/garnet/edit/main/website/docs) folder. You can contribute by making a 
normal GitHub pull request.

### Are Garnet checkpoints guaranteed to work across release versions?

Eventually, yes. However, Garnet is still in active evolution. In particular, we are currently making some core improvements
to the way the log is structured, so the checkpoint and log format may break across versions. We expect the format to stabilize
by version 1.5.

### Is Garnet associated with Redis?
 
No. Like KeyDB and Dragonfly, we were inspired by the popularity and elegance of the RESP API and decided to adopt this 
common standard. Redis is a registered trademark of Redis Ltd. Any rights therein are reserved to Redis Ltd. Any use by
Microsoft is for referential purposes only and does not indicate any sponsorship, endorsement or affiliation between 
Redis and Microsoft.

