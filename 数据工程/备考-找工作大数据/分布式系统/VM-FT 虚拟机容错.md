
论文地址：[The Design of a Practical System for Fault-Tolerant Virtual Machines](https://pdos.csail.mit.edu/6.824/papers/vm-ft.pdf)

参考解读：[解读1](https://www.cnblogs.com/brianleelxt/p/13245754.html)



本论文主要介绍了一个用于提供 容错虚拟机 (fault-tolerant virtual machine) 的企业级商业系统，该系统包含了两台位于不同物理机的虚拟机，其中一台为 primary ，另一台为 backup ，backup 备份了 primary 的所有执行 。
当 primary 出现故障时，backup 可以上线接管 primary 的工作，以此来提供容错 。



主/备份方法中保持状态同步的方法有以下两种：

- State transfer：primary 持续地将所有状态（ 包括 CPU、内存和 I/O 设备 ）变化发送给 backup 。这种方法所需带宽非常大 。
- Replicated state machine：将服务器抽象为确定性状态机 。让 primary 和 backup 按照相同的顺序执接收相同的输入请求，对于不确定的操作（如读取时钟）使用额外的协调来保证主备状态一致 。这种方法实现复杂，但是所需带宽较小 。本文便是采用这种方法 。

==为什么使用虚拟机？==
- 能保证运行在不同虚拟机上的程序接收到一样的不确定性操作。
- 确保该框架在任何操作系统上都能实现。 
![](asset/Pasted%20image%2020230213135346.png)在物理机上确保确定性执行是困难的，因为其会接收到很多不确定输入（ 如定时器中断 ），因此可以采用虚拟机，Hypervisor 对硬件进行模拟和控制，可以捕获到这些不确定输入的所有相关信息，使得 backup 可以重放这些不确定输入 。

### 基本架构
![600](asset/Pasted%20image%2020230213135437.png)

primary 等待来自 backup 的 ACK 时，不会停止执行，只需要延迟输出的发送 （ 异步执行 ）。
VM-FT 检测故障的方式有 UDP 心跳检测和监控 logging channel 中的流量以及 backup 发送给 primary 的 ACK 。若心跳或日志流量停止的时间超过了特定超时时间（ 大约几秒 ），就会声明故障 。


![600](asset/Pasted%20image%2020230213140621.png)
这样的故障检测方法，在有网络故障时，容易遇到 split-brain 问题：即 primary 和 backup 之间通信断开，但此时 primary 还在运行，若 backup 此时上线，会造成两者同时执行的问题，可能会导致数据损坏 。
为解决 split-brain 问题，使 Disk Server 支持 atomic test-and-set 测试，即在 Disk Server 上维护一个 flag ，第一个访问此 flag 的 VM 会成为 primary ，第二个访问此 flag 的 VM 不会上线 。（ 类似于锁 ）
![400](asset/Pasted%20image%2020230213141551.png)

### VM-FT 和 GFS 容错的比较
Vmware 提出的这种 Primary/Backup 方法是分布式容错方法中非常重要的一部分，可以用在许多系统中，不仅仅是分布式存储（GFS 的容错方法），也可以用在分布式计算中，因为它是将所有的操作都记录下来，将它们重新在 Backup 上进行演绎，从而起到了备份的作用，能够做到容错（Fault-Tolerance）。

VM-FT 备份的是 计算，可以用它为任何已有的网络服务器提供容错性。VM-FT 提供了相当严谨的一致性而且对 client 和 server 都是透明的。例如，你可以利用 VM-FT 为已有的邮件服务器提供容错性。

相比之下，GFS 只为 存储 提供容错性。因为 GFS 只针对一种简单的服务提供容错性，它的备份策略会比 VM-FT 更为高效：例如，GFS 不需要使中断发生在所有的副本的同一指令上。GFS 通常只会被用作一个对外提供完整容错服务的系统的一部分：例如，VM-FT 本身也依赖了一个在主备虚拟机间共享的有容错性的存储服务，而你则可以用类似于 GFS 的东西来实现这个模块（ 虽然从细节上来讲 GFS 不太适用于 FT ）。