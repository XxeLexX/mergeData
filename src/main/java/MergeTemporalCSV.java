import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.temporal.io.api.TemporalDataSource;
import org.gradoop.temporal.io.impl.csv.TemporalCSVDataSink;
import org.gradoop.temporal.io.impl.csv.TemporalCSVDataSource;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.util.TemporalGradoopConfig;

public class MergeTemporalCSV {
    public void mergeCSV(String input, String output) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        TemporalDataSource dataSource = new TemporalCSVDataSource(input, TemporalGradoopConfig.createConfig(env));
        TemporalGraph temporalGraph = dataSource.getTemporalGraph();
        TemporalCSVDataSink dataSink = new TemporalCSVDataSink(output, TemporalGradoopConfig.createConfig(env));

        env.setParallelism(1);
        dataSink.write(temporalGraph);

        env.execute();
    }
}
