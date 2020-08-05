import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.*;

import java.io.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MigrateData {
    Query query;
    ReadPropertiesFile readPropertiesFile;
    String folderPath;
    DBConnect dbConnect;
    DataTypeConversion dataTypeConversion;
    BigQuery bigquery;

    public MigrateData(String configPath, String folderPathToSaveData) throws IOException {
        this.query = new Query();
        this.readPropertiesFile = new ReadPropertiesFile(configPath);
        this.dbConnect = new DBConnect(readPropertiesFile.GetHost(),
                readPropertiesFile.GetUserName(), readPropertiesFile.GetPassword());
        this.folderPath = folderPathToSaveData + "\\%s.csv";
        this.dataTypeConversion = new DataTypeConversion();
    }

    public void startMigrateData() throws SQLException, IOException {
        //connecting to PostgreSQL and BigQuery.
        connectToBigQuery();
        dbConnect.Connect();

        //execute the getSchemas query to postgreSQL and store the output in a ResultSet.
        ResultSet schemasResultset = dbConnect.executeQuery(query.getQueryByName("getSchemas"));

        while (schemasResultset.next()) {
            String datasetName = schemasResultset.getString(1);
            Dataset dataset = createDataset(bigquery, datasetName);

            //creating tables
            String getTablesQuery = query.getQueryByName("getTables");
            getTablesQuery = String.format(getTablesQuery, datasetName);
            dbConnect.Reconnect();
            ResultSet tablesResultset = dbConnect.executeQuery(getTablesQuery);
            createTables(dataset, tablesResultset, dataTypeConversion);

            //Copying data from postgreSQL to file
            tablesResultset.beforeFirst();
            copyDataFromSourceToFile(tablesResultset);

            //loading the data from file to BigQuery
            tablesResultset.beforeFirst();
            loadDataFromTable(tablesResultset,datasetName);
        }
    }

    private void connectToBigQuery(){
        //connecting to BQ
        GoogleCredentials credentials = GoogleCredentials.newBuilder().build();
        try (FileInputStream serviceAccountStream =
                     new FileInputStream(readPropertiesFile.GetCredentialsPath())){
            credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        bigquery =
                BigQueryOptions.newBuilder()
                        .setCredentials(credentials)
                        .setProjectId(readPropertiesFile.GetProjectID())
                        .build()
                        .getService();
        //End of connecting
    }

    private void copyDataFromSourceToFile(ResultSet resultSet) throws IOException, SQLException {
        String selectString = query.getQueryByName("select");
        StringBuilder fields = new StringBuilder();
        int num_of_fields;

        while (resultSet.next()) {
            if (resultSet.getString("field_number")
                    .equals(resultSet.getString("number_of_fields"))) {
                fields.append(resultSet.getString("field_name"));
                dbConnect.Reconnect();
                String tableSelect = String.format
                        (selectString, fields.toString(),resultSet.getString("table_name"));
                ResultSet dataResultset = dbConnect.executeQuery(tableSelect);
                fields = new StringBuilder();
                num_of_fields = resultSet.getInt("number_of_fields");
                writeIntoFile(dataResultset, resultSet.getString("table_name"), num_of_fields);
            }else{
                fields.append(resultSet.getString("field_name")).append(",");
            }
        }
    }

    private void writeIntoFile(ResultSet resultSet,String tableName, int num_of_fields)
            throws IOException, SQLException {
        String newLocation = String.format(folderPath,tableName);
        File dataFile = new File(newLocation);
        FileWriter fstream = new FileWriter(dataFile);
        BufferedWriter out = new BufferedWriter(fstream);

        while (resultSet.next()) {
            //write all the data
            for (int i = 1; i <= num_of_fields; i++) {
                if (i == num_of_fields) {
                    if(resultSet.getString(i) != null){
                        out.write(resultSet.getString(i));
                        out.newLine();
                    }else {
                        out.write("");
                        out.newLine();
                    }
                }else {
                    if(resultSet.getString(i) != null){
                        out.write(resultSet.getString(i) + ",");
                    }else{
                        out.write( "" + ",");
                    }
                }
            }
        }
        System.out.println("Complete writing into text file");
        out.close();
    }

    private void loadDataFromTable(ResultSet resultSet,String datasetName) throws SQLException, IOException {
        String schema = "";
        DataTypeConversion dataTypeConversion = new DataTypeConversion();
        while (resultSet.next()){
            if (resultSet.getString("field_number")
                    .equals(resultSet.getString("number_of_fields"))){
                schema += resultSet.getString("fields_schema");
                schema = dataTypeConversion.legacyParser(schema);
                String tableName = resultSet.getString("table_name");
                String fullTableName = datasetName + "." + tableName;
                String newLocation = String.format(folderPath,tableName);
                String command = "cmd /c bq load " + fullTableName + " " + newLocation + " " + schema;
                Runtime.getRuntime().exec(command);
                schema = "";
            }else{
                schema += resultSet.getString("fields_schema") + ",";
            }
        }
    }

    private Dataset createDataset(BigQuery bigquery, String datasetName){
        DatasetInfo datasetInfo = DatasetInfo.newBuilder(datasetName).build();
        return bigquery.create(datasetInfo);
    }

    public  void createTables(Dataset dataset, ResultSet tableResultset, DataTypeConversion dataTypeConversion)
            throws SQLException {
        List<Field> fieldList;
        Map<String, String> fieldTypeMap = new HashMap<>();
        String tableName;
        String fieldName;
        String fieldType;

        while (tableResultset.next()) {
            fieldName = tableResultset.getString("field_name");
            fieldType = tableResultset.getString("field_type");
            fieldType = dataTypeConversion.parser(fieldType);

            if (!tableResultset.getString("field_number")
                    .equals(tableResultset.getString("number_of_fields"))) {
                fieldTypeMap.put(fieldName, fieldType);
            } else {
                tableName = tableResultset.getString("table_name");
                fieldTypeMap.put(fieldName, fieldType);
                fieldList = createFields(fieldTypeMap);
                Schema schema = Schema.of(fieldList);
                fieldTypeMap.clear();
                fieldList.clear();
                TableDefinition tableDefinition = StandardTableDefinition.of(schema);
              dataset.create(tableName, tableDefinition);
            }
        }
    }

    private List<Field> createFields(Map<String,String> fieldTypeMap){
        List<Field> fieldList = new ArrayList<>();

        for ( String key :  fieldTypeMap.keySet() ){
            String type = fieldTypeMap.get(key);
            Field field = Field.newBuilder(key,StandardSQLTypeName.valueOf(type)).build();
            fieldList.add(field);
        }
        return fieldList;
    }
}
