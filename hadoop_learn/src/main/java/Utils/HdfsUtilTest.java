package Utils;

import com.google.common.io.Resources;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by Administrator on 2018/4/7.
 */
public class HdfsUtilTest {
    private Configuration conf;
    @Before
    public void HdfsUtilTest(){
        conf = new Configuration();
        conf.addResource(Resources.getResource("core-site.xml"));
    }
    @Test
    public void HdfsUtilTest2() throws IOException {
        FileSystem fileSystem = FileSystem.get(conf);
        Path path = new Path("/usr");
        boolean flag = fileSystem.exists(path);
        fileSystem.close();
    }
    @Test
    public void HdfsUtilTest3() throws IOException {
       boolean flag =  HdfsUtil.existsFiles(conf,"/user");
        System.out.println(flag);
    }
}
