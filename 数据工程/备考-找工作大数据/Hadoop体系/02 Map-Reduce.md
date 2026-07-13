
- 数据本地化优先

- mapper
	- combiner类: 对单个mapper的结果进行聚合，减少map与reduce之间的网络传输量，属于优化策略。
	- Shuffle：描述数据从map task输出到reduce task输入的这段过程。对map的结果进行分区，排序，汇总等，然后才能传递给reducer执行
- reducer

input ->  map  ->   shuffle  ->  reduce  ->output