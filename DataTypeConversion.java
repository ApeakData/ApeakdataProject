import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class DataTypeConversion {
    Map <String,String> m_DataTypeMap;
    Map <String,String> m_RegexMap;
    Map<String,String> m_LagacySql;

    DataTypeConversion(){
        this.m_DataTypeMap = new HashMap<>();
        this.m_RegexMap = new HashMap<>();
        this.m_LagacySql = new HashMap<>();
        setDataTypeMapping();
        setRegexMapping();
        setLagacySql();
    }

    public void PutDataTypeMapping(String i_Key,String i_Value){
        this.m_DataTypeMap.put(i_Key,i_Value);
    }

    public void PutRegexMapping(String i_Regex, String i_Replace){
        this.m_RegexMap.put(i_Regex,i_Replace);
    }

    private void setDataTypeMapping(){
        this.m_DataTypeMap.put("integer"," int64 ");
        this.m_DataTypeMap.put("bigint"," int64 ");
        this.m_DataTypeMap.put("int"," int64 ");
        this.m_DataTypeMap.put("int4"," int64 ");
        this.m_DataTypeMap.put("boolean","bool");
        this.m_DataTypeMap.put("bool","bool");
        this.m_DataTypeMap.put("varchar(n)","string");
        this.m_DataTypeMap.put("character varying (n)","string");
        this.m_DataTypeMap.put("float8","float64");
        this.m_DataTypeMap.put("date","date");
        this.m_DataTypeMap.put("double precision","float64");
        this.m_DataTypeMap.put("json","struct");
        this.m_DataTypeMap.put("jsonb","struct");
        this.m_DataTypeMap.put("numeric(p,s)","numeric");
        this.m_DataTypeMap.put("real","float64");
        this.m_DataTypeMap.put("int2","int64");
        this.m_DataTypeMap.put("smallint","int64");
        this.m_DataTypeMap.put("text","string");
        this.m_DataTypeMap.put("time[(p)]","time");
        this.m_DataTypeMap.put("timetz","timestamp");
        this.m_DataTypeMap.put("timestamp","timestamp");
        this.m_DataTypeMap.put("timestamptz","timestamp");
        this.m_DataTypeMap.put("xml","struct");
    }

    private void setRegexMapping(){
        this.m_RegexMap.put("integer","INT64");
        this.m_RegexMap.put("bigint"," INT64");
        this.m_RegexMap.put("int","INT64");
        this.m_RegexMap.put("int4","INT64");
        this.m_RegexMap.put("boolean"," BOOL");
        this.m_RegexMap.put("(varchar)+\\(\\d+\\)","STRING");
        this.m_RegexMap.put("(character varying)+\\(\\d+\\)","STRING");
        this.m_RegexMap.put("float8"," FLOAT64");
        this.m_RegexMap.put("double precision","FLOAT64");
        this.m_RegexMap.put("json","STRUCT");
        this.m_RegexMap.put("jsonb","STRUCT");
        this.m_RegexMap.put("numeric\\(.*,.*\\)","NUMERIC");
        this.m_RegexMap.put("real","FLOAT64");
        this.m_RegexMap.put("int2"," INT64");
        this.m_RegexMap.put("smallint"," INT64");
        this.m_RegexMap.put("text"," STRING");
        this.m_RegexMap.put("time\\[\\(.*\\)\\]","TIME");
        this.m_RegexMap.put("timetz","TIMESTAMP");
        this.m_RegexMap.put("timestamp","TIMESTAMP");
        this.m_RegexMap.put("timestamptz"," TIMESTAMP");
        this.m_RegexMap.put("xml"," STRUCT");
        this.m_RegexMap.put(" NULL","");
        this.m_RegexMap.put("\\n","");
        this.m_RegexMap.put("\\s\\s+","");
        this.m_RegexMap.put("date","DATE");
    }

    private void setLagacySql(){
        this.m_LagacySql.put(("(character varying)+\\(\\d+\\)"),"string");
        //this.m_LagacySql.put(("double"),"float");
        this.m_LagacySql.put(("double precision"),"float");
    }
    public String parser(String i_ResultSet){
        String stringAfterParsing = "";
        String stringBeforeParsing ;
        Set set =  this.m_RegexMap.entrySet();
            stringBeforeParsing = i_ResultSet;
            Iterator itr = set.iterator();
            while (itr.hasNext()){
                Map.Entry entry = (Map.Entry)itr.next();
                stringBeforeParsing = stringBeforeParsing.replaceAll(entry.getKey().toString(),entry.getValue().toString());
            }
            stringAfterParsing += stringBeforeParsing;
        return stringAfterParsing;
    }

    public String legacyParser(String i_ResultSet){
        String stringAfterParsing = "";
        String stringBeforeParsing ;
        Set set =  this.m_LagacySql.entrySet();
        stringBeforeParsing = i_ResultSet;
        Iterator itr = set.iterator();
        while (itr.hasNext()){
            Map.Entry entry = (Map.Entry)itr.next();
            stringBeforeParsing = stringBeforeParsing.replaceAll(entry.getKey().toString(),entry.getValue().toString());
        }
        stringAfterParsing += stringBeforeParsing;
        return stringAfterParsing;
    }
}
