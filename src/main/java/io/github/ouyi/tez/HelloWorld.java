package io.github.ouyi.tez;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Arrays;

public class HelloWorld extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(null, new HelloWorld(), args);
        System.exit(res);
    }
}
