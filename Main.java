public class Main  {
    public static void main(String[] args) throws Exception {
        /*PROVIDE THE PATH TO THE CONFIG FILE INCLUDING:
        host,username,password ,projectID ,CredentialsPath
        */
        String configPath = "C:\\Users\\levav\\IdeaProjects\\DBconnection\\resources\\config.properties";
        /*PROVIDE THE PATH TO THE CONFIG FILE INCLUDING:
        tableName_A,tableName_B keys_A ,keys_B,fields_A ,fields_B ,joinType
        */
        String apeakdataPath = "C:\\Users\\levav\\IdeaProjects\\DBconnection\\resources\\apeakdata.properties";

        /*
        MIGRATE DATA FROM POSTGRESQL TO BIGQUERY
        */
        MigrateData migrateData = new MigrateData
                (configPath, "C:\\Users\\levav\\Documents\\data");
        migrateData.startMigrateData();
        /*
        CREATE THE APEAKDATA TABLE OF THE DESIRED TABLES
         */
        ApeakData apeakData = new ApeakData(configPath,"another","check");
        apeakData.createApeakdataTable(apeakdataPath);
        /*
        GET A RECOMMENDATION OF TABLES TO JOIN IN APEAKDATA TABLE BASED ON QUERY HISTORY
        */
        JoinTableRecommendation joinTableRecommendation = new JoinTableRecommendation(configPath);
        joinTableRecommendation.GetRecommendation(2);
        joinTableRecommendation.printTopTables();
    }
}
C:\Users\levav\IdeaProjects\DBconnection\src