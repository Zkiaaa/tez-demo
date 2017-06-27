package io.github.ouyi.tez;

import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.client.CallerContext;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

public class HelloWorld extends Configured implements Tool {

    private static Logger LOGGER = LoggerFactory.getLogger(HelloWorld.class);

    @Override
    public int run(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));

        Configuration conf = getConf();
        TezConfiguration tezConf = new TezConfiguration(conf == null ? new Configuration() : conf);

        TezClient tezClient = TezClient.create(getClass().getSimpleName(), tezConf);
        System.out.println(tezClient);

        String inputPath = args[0];
        String outputPath = args[1];
        int numPartitions = Integer.parseInt(args.length > 2 ? args[2] : "1");
        DAG dag = createDAG(conf, tezConf, inputPath, outputPath, numPartitions);
        System.out.println(dag);

        //runDag(dag, tezClient, LOGGER);
        return 0;
    }

    private DAG createDAG(Configuration conf, TezConfiguration tezConf, String inputPath, String outputPath, int numPartitions) {
        DAG dag = DAG.create("HelloWorld DAG");
        return dag;
    }

    public int runDag(DAG dag, TezClient tezClient, Logger logger) throws TezException,
            InterruptedException, IOException {
        try {
            tezClient.start();
            tezClient.waitTillReady();

            // Set up caller context
            ApplicationId appId = tezClient.getAppMasterApplicationId();
            CallerContext callerContext = CallerContext.create("HelloWorldContext", "Caller id: " + appId, "HelloWorldType", "Tez HelloWorld DAG: " + dag.getName());
            dag.setCallerContext(callerContext);

            // Submit DAG and wait for completion
            DAGClient dagClient = tezClient.submitDAG(dag);
            Set<StatusGetOpts> statusGetOpts = Sets.newHashSet(StatusGetOpts.GET_COUNTERS);
            DAGStatus dagStatus = dagClient.waitForCompletionWithStatusUpdates(statusGetOpts);

            // Check status
            if (dagStatus.getState() == DAGStatus.State.SUCCEEDED) {
                return 0;
            } else {
                logger.info("DAG diagnostics: " + dagStatus.getDiagnostics());
                return -1;
            }
        } finally {
            tezClient.stop();
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(null, new HelloWorld(), args);
        System.exit(res);
    }
}
