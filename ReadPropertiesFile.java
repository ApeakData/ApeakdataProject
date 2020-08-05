import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

public class ReadPropertiesFile {
    FileInputStream fileInputStream;
    Properties properties;

    ReadPropertiesFile(String configPath) throws IOException {
        this.fileInputStream = new FileInputStream(configPath);
        this.properties = new Properties();
        this.properties.load(this.fileInputStream);
    }

    public String GetUserName(){
        return properties.getProperty("username");
    }

    public String GetPassword(){
        return properties.getProperty("password");
    }

    public String GetProjectID(){
        return properties.getProperty("projectID");
    }

    public String GetCredentialsPath(){
        return properties.getProperty("credentialsPath");
    }

    public String GetHost(){
        return properties.getProperty("host");
    }

    public  String GetTableName_A(){
        return properties.getProperty("tableName_A");
    }

    public  String GetTableName_B(){
        return properties.getProperty("tableName_B");
    }

    public  String GetJoinType(){
        return properties.getProperty("joinType");
    }

    public List<String> GetKeyList_A(){
        String keys = properties.getProperty("keys_A");
        String[] keysArr = keys.split(",");
        for(String s : keysArr){
            System.out.println("keysArr A = " + s);
        }
        return new ArrayList<>(Arrays.asList(keysArr));
    }

    public  List<String> GetKeyList_B(){
        String keys = properties.getProperty("keys_B");
        String[] keysArr = keys.split(",");
        for(String s : keysArr){
            System.out.println("keysArr B = " + s);
        }
        return new ArrayList<>(Arrays.asList(keysArr));
    }

    public  List<String> GetFieldList_A(){
        String fields = properties.getProperty("fields_A");
        String[] FieldsArr = fields.split(",");
        for(String s : FieldsArr){
            System.out.println("Field_A = " + s);
        }
        return new ArrayList<>(Arrays.asList(FieldsArr));
    }

    public  List<String> GetFieldList_B(){
        String fields = properties.getProperty("fields_B");
        String[] FieldsArr = fields.split(",");
        for(String s : FieldsArr){
            System.out.println("Field_B = " + s);
        }
        return new ArrayList<>(Arrays.asList(FieldsArr));
    }
}
