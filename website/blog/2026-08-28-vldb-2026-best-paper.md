---
slug: vldb-2026-best-paper
title: Garnet Wins Best Paper Award at VLDB 2026
authors: badrishc
tags: [garnet, research, vldb, paper, award]
---

**The Garnet research paper has received the Best Paper Award at the VLDB 2026 conference!**

The work was presented in Boston. It has been a long and rewarding journey to get here, and this
recognition means a great deal to the whole team and to everyone who supported the project along
the way.

- 📄 **Paper:** [Garnet: A Next-Generation Cache-Store for Accelerating Applications and Services](https://badrish.net/papers/garnet-vldb2026.pdf) (PVLDB 2026)
- 💻 **GitHub:** [microsoft/garnet](https://github.com/microsoft/garnet)

<!--truncate-->

## The paper

*Garnet: A Next-Generation Cache-Store for Accelerating Applications and Services* describes the
design and engineering behind Garnet — a high-performance, remote cache-store that speaks the RESP
wire protocol, runs largely unmodified clients, and delivers order-of-magnitude improvements in
throughput and latency for real-world caching workloads. It is built on top of
[Tsavorite](https://github.com/microsoft/garnet), our storage engine that provides tiered storage,
checkpointing, recovery, and epoch-protected memory reclamation.

The paper was authored by Badrish Chandramouli, Vasileios Zois, Ted Hart, Tal Zaccai, Lukas Maas,
Yoganand Rajasekaran, and Darren Gehring.

## Thank you

This project would not exist without the collaborators, open-source contributors, prior interns,
Garnet users, and everyone who inspired, improved, motivated, and supported it. There are far too
many partners to list, but a few deserve a special shout-out.

For their inspiration and support: Johannes Gehrke, Donald Kossmann, Surajit Chaudhuri, Phil
Bernstein, Knut Risvik, Shireesh Thota, Brendan Burns, Kirill Gavrylyuk, and Hari Sudan S.

For their inputs and collaboration: Venugopal Latchupatula, Alan Yang, Hamdaan Khalid, Alex
Dubinkov, Narayanan Subramanian, Reuben Bond, Kevin Montrose, Tiago Nápoli, Harsha Simhadri, Jack
Moffitt, and many others.

Our prior interns — including Tianyu Li, Sajjad Rahnama, Sambhav Satija, Chenhao Ye, Aneesh Raman,
and Hokeun Cha — contributed to various technical aspects of the larger project.

Thank you all! 🎉
