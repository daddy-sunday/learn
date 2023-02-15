raft协议地址：
  https://github.com/maemual/raft-zh_cn/blob/master/raft-zh_cn.md

1.0版本的定位是一个高可用的key-value存储。但也可以作为高可用的基石，提供master领导能力。再代码实现上对raft算法做了一些优化

当前实现的功能：
  任意节点支持存储数据，
  任意节点支持查询数据，
  任意节点支持刪除数据，
  leader漂移：指定raft组中某个节点为leader节点（该功能是为了后面的分布式存储中负载均衡而开发的）
计划实现但未实现的功能：
  配置变更：动态的添加删除raft节点
  这个功能主要是为后面的多raft组实现的的分布式数据存储做准备。对当前的单raft组意义不大。
运行代码方法：
 执行 test/java目录下的 RaftServiceTest 类里面的测试方法，里面共写了五方法。每个方法对应一个raft节点。测试的时候，可以启动三个或者五个节点（默认是五个节点的配置如果要启动三个节点需要修改配置）。
 执行 test/java目录下的 ZMClientTest 类中的方法，里面有写入数据测试，查询数据测试，删除数据测试，和leader漂移。3个节点可以停掉1个节点，5个节点可以停掉2个节点。缺失的这些节点并不影响raft正常运行，重新启动缺失的节点后会自动同步缺失的数据。
 执行 test/java目录下的 SaveLogTest.findAll() 类中的方法，可以查询所有节点中rocksdb中的存储数据。
 执行 test/java目录下的 SaveLogTest.deleteAll() 类中的方法，可以清空所有节点中rocksdb的存储数据。





