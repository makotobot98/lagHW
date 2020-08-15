package com.lagou.hdfs;

import org.apache.hadoop.fs.Path;
import org.junit.Test;
import java.net.URI;
import java.io.IOException;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class HdfsClientDemo {

    @Test
    public void testMkdirs() throws IOException, InterruptedException,
            URISyntaxException {
        // 1 获取⽂件系统
        Configuration configuration = new Configuration();
        // 配置在集群上运⾏
        // configuration.set("fs.defaultFS", "hdfs://linux121:9000");
        // FileSystem fs = FileSystem.get(configuration);
        FileSystem fs = FileSystem.get(new URI("hdfs://linux121:9000"),
                configuration, "root");
        // 2 创建⽬录
        fs.mkdirs(new Path("/test-client"));
        // 3 关闭资源
        fs.close();
    }

}
