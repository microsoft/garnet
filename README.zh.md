# Garnet
[![.NET CI](https://github.com/microsoft/garnet/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/microsoft/garnet/actions/workflows/ci.yml)
[![](https://img.shields.io/github/release/microsoft/garnet.svg?label=latest%20release&color=007edf)](https://github.com/microsoft/garnet/releases/latest)
[![](https://img.shields.io/nuget/dt/microsoft.garnet.svg?label=nuget%20library&color=007edf&logo=nuget)](https://www.nuget.org/packages/microsoft.garnet)
[![](https://img.shields.io/nuget/dt/garnet-server.svg?label=dotnet%20tool&color=007edf&logo=nuget)](https://www.nuget.org/packages/garnet-server)
[![BDN Charts](https://img.shields.io/badge/BDN%20Charts-8A2BE2)](https://microsoft.github.io/garnet/charts/)
[![Discord Shield](https://discordapp.com/api/guilds/1213937452272582676/widget.png?style=shield)](https://aka.ms/garnet-discord)

<p align="center">
  <a href="README.md">English</a> · <b>简体中文</b>
</p>

Garnet 是一个极速远程缓存与键值存储引擎，具有以下多项独特优势：
* Garnet 以通用的 [RESP](https://redis.io/docs/latest/develop/reference/protocol-spec/) 通信协议作为起点，因此几乎所有主流编程语言中现有的 Redis 客户端均无需任何修改即可直接无缝连接 Garnet，例如 C# 中的 [StackExchange.Redis](https://github.com/StackExchange/StackExchange.Redis)。
* 在面对大量并发客户端连接与小批次请求时，相比同类开源缓存数据库，Garnet 展现出卓越的吞吐量与扩展性，能够为大型应用程序与分布式服务节省可观的算力与服务器成本。
* 在启用了加速网络（Accelerated Networking）的通用云端（Azure）虚拟机上，Garnet 展现出极致低廉的客户端延迟（在 99.9 分位时常低于 300 微秒），这对严苛的实际生产场景至关重要。
* 基于最新 .NET 技术打造，Garnet 具备出色的跨平台能力、高可扩展性与现代化架构。其架构设计兼顾易开发与易演进性，同时绝不妥协常规场景下的极致性能。我们充分利用了 .NET 丰富的类库生态来提供广泛的 API，并预留了充沛的底层优化空间。得益于对 .NET 高性能特性的精细调优，Garnet 在 Linux 和 Windows 上均实现了业界领先的顶尖性能。

本代码库包含构建与运行 Garnet 的完整代码。欲了解更多信息与技术文档，欢迎访问我们的官方网站：[https://microsoft.github.io/garnet](https://microsoft.github.io/garnet)。

**正在寻找全托管云服务？** [Azure Cosmos DB Garnet Cache](https://microsoft.github.io/garnet/docs/azure/overview) 将 Garnet 作为企业级全托管缓存解决方案提供，具备开箱即用的高可用性、性能 SLA 保障及零基础设施运维负担。

## 最新动态 🎉

- **🚀 [向量集合 Vector Sets（预览版）](https://microsoft.github.io/garnet/docs/commands/vector-sets)** —— 基于 [DiskANN](https://github.com/microsoft/DiskANN) 算法与 Garnet 的 Tsavorite 存储引擎实现的近似最近邻（ANN）向量搜索。在我们的[初步基准测试](https://github.com/microsoft/DiskANN/wiki/Perf:-Garnet-Providers-vs-other-Vector-DBs-(Zilliz,-Pinecone,-etc.))中，Garnet 在 QPS、p99 延迟及召回率（Recall）上均表现优异。
- **🔍 [范围索引 Range Index（预览版）](https://microsoft.github.io/garnet/docs/commands/range-index)** —— 由 [Bf-Tree](https://github.com/microsoft/bf-tree) 驱动的针对 Garnet 键的二级范围检索与等值索引支持。
- **📄 Garnet 学术论文已被 VLDB 2026 正式接收！** B. Chandramouli, V. Zois, T. Hart, T. Zaccai, L. M. Maas, Y. Rajasekaran, D. Gehring. *Garnet: A Next-Generation Cache-Store for Accelerating Applications and Services.* PVLDB 2026. [[论文 PDF]](https://www.vldb.org/pvldb/vol19/p224-chandramouli.pdf)

## 特性概览 (Feature Summary)

Garnet 实现了极其广泛的 API 集合，包括原生字符串操作（例如 GET、SET 及键过期设置）、分析型数据类型（例如 HyperLogLog 与 Bitmap）以及复杂对象操作（例如有序集合 Sorted Sets 和列表 Lists）。它支持以客户端 RESP 事务以及服务端 C# 编写的存储过程和扩展模块形式处理多键事务（Multi-key transactions）。用户可以在享受 C# 便捷与安全特性的同时，轻松为原生字符串和自定义对象类型定义扩展操作，大幅降低了开发自定义业务扩展的门槛。Garnet 同时还支持 Lua 脚本。

Garnet 采用了快速且可插拔的网络层设计，为未来接入内核旁路（Kernel-bypass）网络协议栈等前沿技术提供了扩展可能。它通过 .NET 强健的 [SslStream](https://learn.microsoft.com/en-us/dotnet/api/system.net.security.sslstream) 库支持安全的传输层安全协议（TLS）通信，并具备基础的访问控制（ACL）能力。Garnet 名为 Tsavorite 的底层存储引擎专为极致性能构建，拥有诸多企业级数据库特性，如多线程扩展性、分层存储支持（内存、SSD 以及云存储）、快速非阻塞检查点机制（Checkpointing）、故障恢复、确保持久性的操作日志记录（Operation logging）、多键事务支持，以及出色的内存生命周期管理与复用机制。最后，Garnet 还支持集群（Cluster）运行模式，提供数据分片（Sharding）、主从复制（Replication）以及动态键迁移功能。

<p align="center">
  <img src="https://github.com/microsoft/garnet/assets/18355833/851be90b-e43a-40ca-ae56-7dc087cf6adc" width=350 />
</p>

## 性能表现一览 (Performance Preview)

我们在[官方网站](https://microsoft.github.io/garnet/docs/benchmarking/overview)上展示了多个核心基准测试结果，对比了 Garnet 与业界主流开源缓存数据库的性能表现。

## 核心架构设计亮点 (Design Highlights)

Garnet 的设计重塑了整个缓存存储技术栈——从网络层接收数据包、解析与执行数据库命令，一直到底层执行存储读写交互。这些创新凝聚了我们[多年来前期研究成果](https://microsoft.github.io/garnet/docs/research/papers)的结晶。下图展示了 Garnet 的总体系统架构：

<p align="center">
  <img src="https://github.com/microsoft/garnet/assets/18355833/8d89f719-f86b-4b1f-81d1-1ae7bd450001" width=400 />
</p>

Garnet 的网络层基于共享内存设计，TLS 解密处理与存储交互直接在网络 IO 完成线程上就地执行，避免了绝大部分常规场景下的线程切换开销。这种架构允许利用 CPU 缓存一致性将数据直接送达处理逻辑，彻底摒弃了传统需要将数据重排洗牌（Shuffle）转移至服务端对应特定分片的设计。

Garnet 的存储设计包含两个由统一步骤操作日志（Unified operation log）绑定的 Tsavorite 键值存储引擎。第一个引擎被称为“主存储（Main store）”，针对原生字符串操作进行了深度优化，并通过精密的内存池管理规避了垃圾回收（GC）暂停。第二个可选的“对象存储（Object store）”则针对复杂对象与自定义数据类型进行了优化，涵盖了诸如 Sorted Set、Set、Hash、List 及 Geo 等热门类型。对象存储中的数据结构当前实现充分复用了 .NET 类库生态，在内存堆上存储以获得极高的更新效率，同时在磁盘上以序列化格式持久化。未来，我们计划进一步探索统一索引与日志架构，以降低长期维护成本。

Garnet 架构设计的一大鲜明特征在于其“细腰（Narrow-waist）”形态的 Tsavorite 存储 API，在此之上构建起了丰富庞大且高度可扩展的 RESP API 生态。该底层 API 仅包含读取（Read）、更新插入（Upsert）、删除（Delete）以及原子读-改-写（Read-modify-write）四种核心原语，并通过异步回调机制允许 Garnet 在每个操作的不同生命周期阶段插入定制逻辑。这种分层 API 抽象使我们能够将解析与查询处理层，与底层的并发控制、分层存储及检查点等存储细节清晰地彻底解耦。Garnet 使用两阶段锁（Two-phase locking）来保障多键事务的一致性。

### 集群模式 (Cluster Mode)

除单节点运行外，Garnet 还提供了功能完备的集群模式，支持用户构建和管理带数据分片与高可用副本的分布式集群。Garnet 还支持高效动态的数据迁移方案，以便在节点间重新平衡分片。用户可以使用标准的 Redis 集群管理命令来配置与运维 Garnet 集群，各个节点之间通过 Gossip 协议同步并演进集群状态元数据。Garnet 当前的集群设计属于*被动式（passive）*架构：这意味着它内部不运行分布式 Leader 选举算法，而是直接响应用户提供的外部*控制平面（Control plane）*下发的操作指令；详情请参阅[此文档链接](https://microsoft.github.io/garnet/docs/cluster/overview#control-plane)。

## 后续步骤 (Next Steps)

欢迎访问我们的[官方文档](https://microsoft.github.io/garnet)站点，或直接查阅[快速入门 (Getting Started)](https://microsoft.github.io/garnet/docs/getting-started)与[版本发布说明 (Releases)](https://microsoft.github.io/garnet/docs/welcome/releases)板块。

## 开源许可证 (License)

本项目遵循 [MIT 许可证](https://opensource.org/licenses/MIT)，详情请参阅 [LICENSE](LICENSE) 文件。

## 隐私说明 (Privacy)

关于微软隐私政策的详细信息，请访问 [https://privacy.microsoft.com/zh-cn/privacystatement](https://privacy.microsoft.com/zh-cn/privacystatement)。

## 参与贡献 (Contributing)

本项目诚挚欢迎社区贡献与建议。绝大多数贡献都需要您签署一份贡献者许可协议（CLA），声明您有权且确实授予我们使用您所贡献内容的权利。详情请访问 https://cla.opensource.microsoft.com。

当您提交拉取请求（PR）时，CLA 机器人会自动判断您是否需要签署 CLA，并在 PR 上标注适当状态（例如状态检查或留言）。只需按照机器人的指引操作即可。对于使用微软 CLA 的所有代码库，您只需签署一次。

本项目已采纳 [Microsoft 开源行为准则](https://opensource.microsoft.com/codeofconduct/)。如需了解更多信息，请参阅[行为准则常见问题解答](https://opensource.microsoft.com/codeofconduct/faq/)，如有其他疑问或意见，亦可联系 [opencode@microsoft.com](mailto:opencode@microsoft.com)。

## 商标声明 (Trademarks)

本项目可能包含项目、产品或服务的商标或徽标。对微软商标或徽标的授权使用受制于且必须遵循 [微软商标与品牌使用指南](https://www.microsoft.com/en-us/legal/intellectualproperty/trademarks/usage/general)。在对本项目修改后的衍生版本中使用微软商标或徽标时，绝不得引起混淆或暗示获得微软的赞助与背书。对第三方商标或徽标的任何使用均须遵守相应第三方的各项品牌政策。

Redis 是 Redis Ltd. 的注册商标。其中包含的任何权利均归 Redis Ltd. 所有。微软在此处的任何提及仅供参考指引之用，绝不代表 Redis 与微软之间存在任何赞助、背书或从属关系。

---

> **翻译说明（Translation Disclaimer）**
> 本文档由社区志愿者（[@JasonYeYuhe](https://github.com/JasonYeYuhe)）人工翻译并维护，旨在降低中文开发者的阅读门槛。最后同步日期：2026年10月18日。若译文与英文原版存在任何出入或滞后，请以 [英文原版 (README.md)](README.md) 为准。欢迎通过 PR 协助改进译文！
