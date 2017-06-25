package io.github.ouyi.tez;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;

import java.io.IOException;
import java.util.Arrays;

public class HelloWorld extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        TezClient tezClient = createTezClient(new TezConfiguration(getConf() == null ? new Configuration() : getConf()));
        System.out.println(tezClient);
        System.out.println(Arrays.toString(args));
        return 0;
    }

    private TezClient createTezClient(TezConfiguration tezConf) throws IOException, TezException {
        TezClient tezClient = TezClient.create(getClass().getSimpleName(), tezConf);
        tezClient.start();
        return tezClient;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(null, new HelloWorld(), args);
        System.exit(res);
    }
}
