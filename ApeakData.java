import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.*;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

public class ApeakData {
    BigQuery bigQuery;
    ReadPropertiesFile readPropertiesFile;
    final String  datasetName;
    final String tableName;

    ApeakData(String configPath,String newTableName) throws IOException {
        this.readPropertiesFile = new ReadPropertiesFile(configPath);
        this.datasetName = "apeakdata";
        this.tableName = newTableName;
    }

    ApeakData(String configPath,String datasetName, String newTableName) throws IOException {
        this.readPropertiesFile = new ReadPropertiesFile(configPath);
        this.datasetName = datasetName;
        this.tableName = newTableName;
    }

    public void createApeakdataTable(String apeakdataPropertiesPath) throws Exception {
        ReadPropertiesFile reader = new ReadPropertiesFile(apeakdataPropertiesPath);
        createApeakdataTable(reader.GetTableName_A(),reader.GetTableName_B(),reader.GetKeyList_A(),reader.GetKeyList_B()
        ,reader.GetFieldList_A(),reader.GetFieldList_B(),reader.GetJoinType());
    }

    public void createApeakdataTable(String tableName_A, String tableName_B, List<String> Akeys,
                                     List<String> Bkeys, List<String> selectFieldsA,
                                     List<String> selectFieldsB, String joinType) throws Exception {
        String query = "select ";
        if(Akeys.size() != Bkeys.size()){
            throw new  Exception("key lists must be the same size!");
        }else {
            query += addStringListToString(selectFieldsA);
            query += ", " + addStringListToString(selectFieldsB);
            query += " from " + tableName_A + " " + joinType + " " + tableName_B + " on ";
            query += createKeysToKeysString(Akeys,Bkeys);
            //run the query script on BigQuery
            saveQueryToTable(query);
        }
    }

    private void createDataset(){
        DatasetInfo datasetInfo = DatasetInfo.newBuilder(datasetName).build();
        bigQuery.create(datasetInfo);
    }

    private void createTable() {
        try {
            TableId tableId = TableId.of(datasetName, tableName);
            Schema schema = Schema.of();
            TableDefinition tableDefinition = StandardTableDefinition.of(schema);
            TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
            bigQuery.create(tableInfo);
            System.out.println("Table created successfully");
        } catch (BigQueryException e) {
            System.out.println("Table was not created. \n" + e.toString());
        }
    }

    public String createKeysToKeysString(List<String> keysA, List<String> keysB){
        StringBuilder res = new StringBuilder();

        for (int i = 0; i < keysA.size();i++){
            if(i == keysA.size() - 1){
                res.append(keysA.get(i)).append(" = ").append(keysB.get(i));
            }else{
                res.append(keysA.get(i)).append(" = ").append(keysB.get(i)).append(" and ");
            }
        }

        return res.toString();
    }

    public String addStringListToString(List<String> stringList){
        StringBuilder res = new StringBuilder();
        int i = 0;

        for(String str: stringList){
            if(i == stringList.size() - 1){
                res.append(str);
            }else {
                res.append(str).append(", ");
            }
        }

        return res.toString();
    }

    public void saveQueryToTable(String queryString) {
        try {
           // Initialize client that will be used to send requests. This client only needs to be created
           // once, and can be reused for multiple requests.
            connectToBigQuery();
            //create the new apeakdata dataset for all the joined tables.
            createDataset();
            //create the new table for the joined table.
            createTable();
            // Initialize client that will be used to send requests. This client only needs to be created
            // once, and can be reused for multiple requests.

            // Identify the destination table
            TableId destinationTable = TableId.of(datasetName, tableName);

            // Build the query job
            QueryJobConfiguration queryConfig =
                    QueryJobConfiguration.newBuilder(queryString).setDestinationTable(destinationTable).build();

            // Execute the query.
            bigQuery.query(queryConfig);

            // The results are now saved in the destination table.

            System.out.println("Saved query ran successfully");
        } catch (BigQueryException | InterruptedException e) {
            System.out.println("Saved query did not run \n" + e.toString());
        }

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
