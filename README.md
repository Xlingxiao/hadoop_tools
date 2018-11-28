# hadoop_tools
#WorldCount
这个程序是hadoop中非常具有代表性的一个程序，这里会着重的介绍
通过这个程序我们应该可以理解
1.hadoop在分布式程序中为我们做了什么？

2.hadoop的程序是如何构成的？

3.如何写一个hadoop程序？

4.正确认识hadoop

首先hadoop在分布式程序中为我们做了什么？

在学习hadoop之初通常我们会接触到很多hadoop的理论知识，而且大部分都是理论知识，
通过这些理论知识我们往往很难将这些理论和hadoop程序的编写构建起联系来，
学习hadoop中我一直在考虑，hadoop的作用，以及hadoop理论和hadoop编程的区别
现在有了一点大体的了解：hadoop在分布式程序中为我们做的主要的一点是
hadoop让我们将普通的程序以集群的计算能力进行计算。我们在编写Hadoop程序的时候
不需要考虑程序是如何放到集群上运行的，只需要按照hadoop的程序结构编写java程序
就可以轻松的使用集群的计算能力。

2.hadoop的程序如何构成？
hadoop的程序主要由两个方面构成 Mapper 和 Reducer 这里和之前学习hadoop的理论
是一样的，还有一个对hadoop job 的配置，我们在编写程序的时需要分别写好
hadoop的Mapper 和 Reducer。然后在主要的类中将mapper 和 reducer 进行应用
配置、启动job，程序就开始启动了。

3.运行流程：
    
    3.1：在job中配置Configuration，和工作的名称
    3.2：在job中配置mapper和reducer类
    3.3：配置mapper的输入文件的类型
    3.4：配置mapper端输出的key-value的类型
    3.5：配置job输出的key-value的类型
    3.6：指定job的输入文件/文件夹
    3.7：指定job的输出文件夹（不用且不能预先创建）
    3.8：job.waitForCompletion(true) 执行
Mapper类来说输入的key-value就相当于整个输入的文件数据，并且我们不需要管
hadoop是如何输入的数据传给这个mapper的，hadoop自动为我们将输入的数据都装载
到了mapper的输入中的key-value中了。mapper中会对每行数据执行map函数，map的
输入数据也就是读取的每行数据。
将需要输出的数据放到context对象中。reducer会自动的收到这些map阶段的数据。
Reducer类：它获得的key是唯一的，而value是一个迭代对象，是所有key相同的value
的集合。reducer会自动运行根据每个唯一的key值运行一次reduce函数。最后reducer
将reducer阶段的结果写入context中，hadoop会自动将放进去的数据进行输出。
    
4.正确认识hadoop
hadoop是一个大数据平台的计算框架，它并不是一个神奇的与其他java程序完全不同
的东西，hadoop程序或者说MapReduce程序只是帮我们吧集群计算能力和集群的管理
的工作做了，使我们编写程序的时候只需要专注于完成我们的目标就可以了，而不需要
考虑如何将一个程序的运行使用整个集群来执行。    