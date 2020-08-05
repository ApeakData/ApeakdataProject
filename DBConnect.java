import java.sql.*;


public class DBConnect {
    private String m_Host;
    private String m_UserName;
    private String m_Password;
    Connection m_Connection;
     Statement m_Statement;

    DBConnect(String i_Host, String i_UserName, String i_Password){
        this.m_Host = i_Host;
        this.m_Password = i_Password;
        this.m_UserName = i_UserName;
    }
    String getHostName(){ return m_Host; }
    String getUserName(){ return m_UserName; }
    String getPassword(){ return m_Password; }
    Connection getConnection(){ return m_Connection; }
    Statement getStatement(){ return m_Statement; }

     public void Connect(){
         try{
             this.m_Connection = DriverManager.getConnection(this.m_Host,this.m_UserName,this.m_Password);
             this.m_Statement = m_Connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE , ResultSet.CONCUR_READ_ONLY);
         } catch (SQLException err){
             System.out.println(err.getMessage());
         }
    }

    public void Reconnect() throws SQLException {
        try{
            if(!this.m_Connection.isClosed()){
                this.m_Connection.close();
                this.Connect();
            }
        }catch(Exception e){
            throw new SQLException(e.getMessage());
        }

    }

    public ResultSet executeQuery(String i_Query) throws SQLException {

       return this.m_Statement.executeQuery(i_Query);
    }
}

