
- 分布式文件管理系统
- 不支持修改，但支持追加操作，依次保持文件的一致性. 
![](asset/Pasted%20image%2020230210155859.png)




- 流水线同步 
- 数据流和控制流分离
- 如何保持一致性
	- master指定primary chunk server, 它负责指定副本的写入顺序，然后其他chunk server 参照这个顺序
	- 每个chunk都有checksum
	- 每个chunk都有一个 版本号
	- GFS推荐更多地使用追加操作来保证文件地一致性。
![600](asset/Pasted%20image%2020230210201318.png)
