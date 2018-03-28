1、BaseBasicBolt 和 BaseRichBolt 的区别
    BaseBasicBolt通过BasicOutputCollector自动ack
    BaseRichBolt需要手动ack，不然会造成内存溢出
2、tuple部分消息处理失败，spout重发整个tuple，怎么保证成功的子tuple不重复处理
    storm原生api无法支持这种事物操作,可以使用storm提供的高级api-trident来做到(根据分布式协议比如两阶段提交协议等)，或者我们自身根据业务来做，比如入库之前先查询是否有记录
3、去掉storm可靠性的方法
    1）、Config.TOPOLOGY_ACKERS 设置成 0. 在这种情况下， storm会在spout发射一个tuple之后马上调用spout的ack方法。也就是说这个tuple树不会被跟踪。
    2）、在tuple层面去掉可靠性。 你可以在发射tuple的时候不指定messageid来达到不跟粽某个特定的spout tuple的目的。
    3）、对于一个tuple树里面的某一部分到底成不成功不是很关心，那么可以在发射这些tuple的时候unanchor它们。 这样这些tuple就不在tuple树里面， 也就不会被跟踪了。
4、ack机制的限流作用
    为了避免spout发送数据太快，而bolt处理太慢，常常设置pending数，当spout有等于或超过pending数的tuple没有收到ack或fail响应时，跳过执行nextTuple， 从而限制spout发送数据。
    通过conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, pending);设置spout pend数。