import java.util.HashMap;
import java.util.Map;

public class Query {
    Map<String,String> queryMap;

    public Query(){
        this.queryMap = new HashMap<String, String>();
        this.init();
    }

    public Map<String, String> getQueryMap() {
        return queryMap;
    }

    public void setQueryMap(Map<String, String> queryMap) {
        this.queryMap = queryMap;
    }

    public String getQueryByName(String queryName){
        try{
            if(this.queryMap.containsKey(queryName)){
                return this.queryMap.get(queryName);
            }
        }catch (IllegalArgumentException e){
            throw new IllegalArgumentException("No such Query Exist");
        }
        return null;
    }

    public void put(String queryName, String query)  {
        try{
            if(!this.queryMap.containsKey(queryName)){
                this.queryMap.put(queryName,query);
            }
        }catch (IllegalArgumentException queryNameExists){
            throw  new IllegalArgumentException("Query Name Already exists");
        }
    }

    private void init(){
        put("getSchemas","select distinct nspname \n" +
                "from pg_catalog.pg_namespace pn\n" +
                "where pn.nspname not in ('information_schema') and pn.nspname not like 'pg%'");

        put("getTables","SELECT \n" +
                "\n" +
                "    c.RELNAME as table_Name, a.attname AS field_Name,\n" +
                "\n" +
                "    pg_catalog.format_type(a.atttypid, a.atttypmod) as field_type,\n" +
                "    \n" +
                "    a.attname||':'||pg_catalog.format_type(a.atttypid, a.atttypmod) as fields_schema, \n" +
                "\n" +
                "    row_number () OVER( partition by C.relname order by   a.attname\n" +
                "\n" +
                "    )as field_Number, COUNT(*) over (partition by  C.relname) as number_Of_Fields\n" +
                "\n" +
                "\t  FROM pg_class c,\n" +
                "\n" +
                "   pg_attribute a,\n" +
                "\n" +
                "   pg_type t, \n" +
                "\n" +
                "   pg_catalog.pg_namespace pn\n" +
                "\n" +
                "  WHERE 1=1\n" +
                "\n" +
                "   and pn.oid=c.relnamespace \n" +
                "\n" +
                "   AND a.attnum > 0\n" +
                "\n" +
                "   AND a.attrelid = c.oid\n" +
                "\n" +
                "   AND a.atttypid = t.oid\n" +
                "\n" +
                "   and pn.nspname = '%s'\n" +
                "\n" +
                " ORDER by 1,2,3");
        put("select","select" +
                " %s from %s;\n");
        put("number_of_columns","select \n" +
                "count(distinct a.attname) as num_of_columns\n" +
                "from pg_class c, pg_attribute as a\n" +
                "  WHERE 1=1\n" +
                "   AND a.attnum > 0\n" +
                "   AND a.attrelid = c.oid\n" +
                "   and c.RELNAME ='%s'");
    }
}
