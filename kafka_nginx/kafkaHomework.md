使用Kafka做日志收集。

需要收集的信息：

1、用户ID（user_id）

2、时间（act_time）

3、操作（action，可以是：点击：click，收藏：job_collect，投简历：cv_send，上传简历：cv_upload）

4、对方企业编码（job_code）

 

 

1、HTML可以理解为拉勾的职位浏览页面

2、Nginx用于收集用户的点击数据流，记录日志access.log

3、将Nginx收集的日志数据发送到Kafka主题：tp_individual

 

架构：

HTML+Nginx+ngx_kafka_module+Kafka

提示：

学员需要自己下载nginx，配置nginx的ngx_kafka_module，自定义一个html页面，能做到点击连接就收集用户动作数据即可。

作业需提交：

1、html的截图+搭建的过程+结果截图以文档或视频演示形式提供。





# Solution
## Approach1: nginx -> flume -> kafka
- https://blog.csdn.net/weixin_45896475/article/details/104866712
  - https://blog.csdn.net/weixin_45896475/article/details/104876946
## Approach2: nginx -> ngx_kafka_module -> kafka
- https://www.fatalerrors.org/a/nginx-integrates-kafka.html
- https://blog.51cto.com/simplelife/2307998

## Reference
- https://blog.csdn.net/fanfan4569/article/details/108717809
- https://www.yuque.com/litao-nubyb/oy7dss/nlbkvu
- **html path on ngnix**: https://docs.nginx.com/nginx/admin-guide/web-server/web-server/
- **JQuery tutorial**: https://www.w3schools.com/jquery/