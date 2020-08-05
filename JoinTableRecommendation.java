import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class JoinTableRecommendation {
    BigQuery bigQuery;
    ReadPropertiesFile readPropertiesFile;
    Map<String,Integer> topTables;

    JoinTableRecommendation(BigQuery bigQuery){
        this.bigQuery = bigQuery;
        this.topTables = new HashMap<>();
    }

    JoinTableRecommendation(String credentialsPath) throws IOException {
        readPropertiesFile = new ReadPropertiesFile(credentialsPath);
        connectToBigQuery();
        this.topTables = new HashMap<>();
    }
    public void printTopTables(){
        for(String s : topTables.keySet()){
            System.out.println("Table to join = ***" + s + "*** Number of previous joins = " + topTables.get(s));
        }
    }

    public void GetRecommendation(int pageLimit) throws IOException {
        Page<Job> jobs = bigQuery.listJobs(BigQuery.JobListOption.pageSize(pageLimit));
        int joinCount;
        String str;
            System.out.println("LOADING...");
            for (Job job : jobs.iterateAll()) {
                //check if the job is of type `query and if the query is a valid query
                if(job.getConfiguration().getType().equals(JobConfiguration.Type.valueOf("QUERY")) && job.getStatus().getError() == null){
                    QueryJobConfiguration queryJobConfiguration = job.getConfiguration();
                    String queryString = queryJobConfiguration.getQuery();

                    if(queryString.contains("select")){
                        if(queryString.contains("join")) {
                            str = getTableNames(queryString);
                            //add only unique strings
                            if (!topTables.containsKey(str)) {
                                topTables.put(str, 1);
                            }else{
                                joinCount = topTables.get(str);
                                joinCount++;
                                topTables.replace(str,joinCount);
                            }
                            //check if after the `from statement there is a join of at least 2 tables
                            // separated by comma.
                            if (queryString.contains("from") && queryString.contains("where")) {
                                str = getTablesByComma(queryString);
                                //add only unique strings
                                if (!topTables.containsKey(str) && str != null) {
                                    topTables.put(str,1);
                                }else{
                                    joinCount = topTables.get(str);
                                    joinCount++;
                                    topTables.replace(str,joinCount);
                                }
                            }
                        }
                    }
                }
            }
    }


    private String getTablesByComma(String queryString){
        StringBuilder stringBuilder = new StringBuilder();
        String[] str = queryString.split(" from ");
        str[1] = str[1].replaceAll("\\s*,\\s*",",");
        str[1] = str[1].replaceAll("where .*","");
        str[1] = str[1].replaceAll("\\s+.*?,",",");
        str = str[1].split(",");
        //check if there is a 'comma' join statement between at least 2 tables
        if(str.length < 2){
            return null;
        }
        for(int i = 0; i < str.length; i++){
            if(i == str.length - 1){
               stringBuilder.append(str[i].replaceAll("\\s+.+",""));
            }else{
                stringBuilder.append(str[i] + " ");
            }
        }
        return stringBuilder.toString();
    }

    private String getTableNames(String queryString){
        String tableA = getTableA(queryString);
        String tableB = getTableB(queryString);
        return tableA + " " + tableB;
    }

    private  String getTableA(String queryString){
        String[] strArr = queryString.split("from ");
        strArr = strArr[1].split(" ");

        return strArr[0];
    }

    private  String getTableB(String queryString){
        String[] strArr = queryString.split("join ");
        strArr = strArr[1].split(" ");

        return strArr[0];
    }

    private void connectToBigQuery(){
        //connecting to BQ
        GoogleCredentials credentials = GoogleCredentials.newBuilder().build();
        try (FileInputStream serviceAccountStream =
                     new FileInputStream(readPropertiesFile.GetCredentialsPath())) {
            credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        bigQuery =
                BigQueryOptions.newBuilder()
                        .setCredentials(credentials)
                        .setProjectId(readPropertiesFile.GetProjectID())
                        .build()
                        .getService();
        //End of connecting
    }
}
