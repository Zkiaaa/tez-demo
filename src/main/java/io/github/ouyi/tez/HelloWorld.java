package io.github.ouyi.tez;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.client.CallerContext;
import org.apache.tez.client.TezClient;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.dag.api.*;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.hadoop.shim.HadoopShim;
import org.apache.tez.hadoop.shim.HadoopShimsLoader;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.mapreduce.processor.SimpleMRProcessor;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.api.KeyValuesReader;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVEdgeConfig;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;
import org.apache.tez.runtime.library.processor.SimpleProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.StringTokenizer;

public class HelloWorld extends Configured implements Tool {

    private static final String INPUT = "Input";
    private static final String TOKENIZER_VERTEX = "TokenizerVertex";
    private static final String SUMMATION_VERTEX = "SummationVertex";
    private static final String OUTPUT = "Output";

    private static Logger LOGGER = LoggerFactory.getLogger(HelloWorld.class);

    public static class TokenProcessor extends SimpleProcessor {
        private final IntWritable one = new IntWritable(1);
        private final Text word = new Text();

        public TokenProcessor(ProcessorContext context) {
            super(context);
        }

        @Override
        public void run() throws Exception {
            Preconditions.checkArgument(getInputs().size() == 1);
            Preconditions.checkArgument(getOutputs().size() == 1);
            KeyValueReader kvReader = (KeyValueReader) getInputs().get(INPUT).getReader();
            KeyValueWriter kvWriter = (KeyValueWriter) getOutputs().get(SUMMATION_VERTEX).getWriter();
            while (kvReader.next()) {
                StringTokenizer itr = new StringTokenizer(kvReader.getCurrentValue().toString());
                while (itr.hasMoreTokens()) {
                    word.set(itr.nextToken());
                    kvWriter.write(word, one);
                }
            }
        }
    }

    public static class SumProcessor extends SimpleMRProcessor {

        public SumProcessor(ProcessorContext context) {
            super(context);
        }

        @Override
        public void run() throws Exception {
            Preconditions.checkArgument(getInputs().size() == 1);
            Preconditions.checkArgument(getOutputs().size() == 1);
            KeyValueWriter kvWriter = (KeyValueWriter) getOutputs().get(OUTPUT).getWriter();
            KeyValuesReader kvReader = (KeyValuesReader) getInputs().get(TOKENIZER_VERTEX).getReader();
            while (kvReader.next()) {
                Text word = (Text) kvReader.getCurrentKey();
                int sum = 0;
                for (Object value : kvReader.getCurrentValues()) {
                    sum += ((IntWritable) value).get();
                }
                kvWriter.write(word, new IntWritable(sum));
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        LOGGER.info(Arrays.toString(args));
        String inputPath = args[0];
        String outputPath = args[1];
        boolean localMode = Boolean.parseBoolean(args[2]);
        int numPartitions = Integer.parseInt(args.length > 3 ? args[3] : "1");

        Configuration conf = Optional.ofNullable(getConf()).orElse(new Configuration());
        TezConfiguration tezConf = new TezConfiguration(conf);

        if (localMode) {
            tezConf.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, localMode);
        }

        TezClient tezClient = TezClient.create(getClass().getSimpleName(), tezConf);
        LOGGER.info(tezClient.toString());

        DAG dag = createDAG(inputPath, outputPath, numPartitions, tezConf);
        LOGGER.info(dag.toString());

        runDag(dag, tezClient, tezConf);
        return 0;
    }

    private DAG createDAG(String inputPath, String outputPath, int numPartitions, TezConfiguration conf) {
        DataSourceDescriptor dataSource = MRInput.createConfigBuilder(new Configuration(conf), TextInputFormat.class, inputPath)
                .groupSplits(!isDisableSplitGrouping())
                .generateSplitsInAM(!isGenerateSplitInClient())
                .build();
        LOGGER.info(dataSource.toString());
        Vertex tokenizerVertex = Vertex.create(TOKENIZER_VERTEX, ProcessorDescriptor.create(TokenProcessor.class.getName()))
                .addDataSource(INPUT, dataSource);
        LOGGER.info(tokenizerVertex.toString());

        DataSinkDescriptor dataSink = MROutput.createConfigBuilder(new Configuration(conf), TextOutputFormat.class, outputPath)
                .build();
        LOGGER.info(dataSink.toString());
        Vertex summationVertex = Vertex.create(SUMMATION_VERTEX, ProcessorDescriptor.create(SumProcessor.class.getName()), numPartitions)
                .addDataSink(OUTPUT, dataSink);
        LOGGER.info(summationVertex.toString());

        OrderedPartitionedKVEdgeConfig edgeConfig = OrderedPartitionedKVEdgeConfig.newBuilder(Text.class.getName(), IntWritable.class.getName(), HashPartitioner.class.getName())
                .setFromConfiguration(conf)
                .build();
        LOGGER.info(edgeConfig.toString());

        DAG dag = DAG.create("HelloWorld DAG")
                .addVertex(tokenizerVertex)
                .addVertex(summationVertex)
                .addEdge(Edge.create(tokenizerVertex, summationVertex, edgeConfig.createDefaultEdgeProperty()));
        LOGGER.info(dag.toString());

        return dag;
    }

    private boolean isGenerateSplitInClient() {
        return false;
    }

    private boolean isDisableSplitGrouping() {
        return false;
    }

    public int runDag(DAG dag, TezClient tezClient, TezConfiguration tezConf) throws TezException,
            InterruptedException, IOException {
        try {
            tezClient.start();
            tezClient.waitTillReady();

            // Set up caller context
            ApplicationId appId = tezClient.getAppMasterApplicationId();
            CallerContext callerContext = CallerContext.create("HelloWorldContext", "Caller id: " + appId, "HelloWorldType", "Tez HelloWorld DAG: " + dag.getName());
            HadoopShim hadoopShim = new HadoopShimsLoader(tezConf).getHadoopShim();

            if (appId != null) {
                TezUtilsInternal.setHadoopCallerContext(hadoopShim, appId);
                callerContext.setCallerIdAndType(appId.toString(), "TezExampleApplication");
            }
            dag.setCallerContext(callerContext);

            // Submit DAG and wait for completion
            DAGClient dagClient = tezClient.submitDAG(dag);
            Set<StatusGetOpts> statusGetOpts = Sets.newHashSet(StatusGetOpts.GET_COUNTERS);
            DAGStatus dagStatus = dagClient.waitForCompletionWithStatusUpdates(statusGetOpts);

            // Check status
            if (dagStatus.getState() == DAGStatus.State.SUCCEEDED) {
                return 0;
            } else {
                LOGGER.info("DAG diagnostics: " + dagStatus.getDiagnostics());
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
