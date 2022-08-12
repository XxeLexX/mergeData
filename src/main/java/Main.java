import java.util.ArrayList;

public class Main {
    public static void main(String[] args) throws Exception {

        ArrayList<String> inputs = new ArrayList<>();
        String citibike_1 = "hdfs:///citibike/gradoop_SF1";
        String citibike_10 = "hdfs:///citibike/gradoop_SF10";
        String citibike_100 = "hdfs:///citibike/gradoop_SF100";
        inputs.add(citibike_1);
        inputs.add(citibike_10);
        inputs.add(citibike_100);

        String ldbc_1 = "hdfs:///ldbc/csv_gradoop_temporal/ldbc_1";
        String ldbc_10 = "hdfs:///ldbc/csv_gradoop_temporal/ldbc_10";
        String ldbc_100 = "hdfs:///ldbc/csv_gradoop_temporal/ldbc_100";
        inputs.add(ldbc_1);
        inputs.add(ldbc_10);
        inputs.add(ldbc_100);

        String stackexchange_chess = "hdfs:///stackexchange/chess/csv_gradoop_temporal";
        String stackexchange_math = "hdfs:///stackexchange/math/csv_gradoop_temporal";
        String stackexchange_writers = "hdfs:///stackexchange/writers/csv_gradoop_temporal";
        inputs.add(stackexchange_chess);
        inputs.add(stackexchange_math);
        inputs.add(stackexchange_writers);

        String stackoverflow = "hdfs:///stackoverflow/gradoop_csv_temporal";
        inputs.add(stackoverflow);

        for (String input: inputs){
            String subFile = input.substring(8);
            String hdfsOUT = "hdfs:///user/xl59hozy/" + subFile;
            MergeTemporalCSV mergeTemporalCSV = new MergeTemporalCSV();
            mergeTemporalCSV.mergeCSV(input, hdfsOUT);
            System.out.println(subFile + " merged");
        }
    }
}
