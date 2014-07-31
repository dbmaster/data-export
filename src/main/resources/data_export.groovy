import groovy.sql.Sql
import com.branegy.dbmaster.database.api.ModelService;
import com.branegy.dbmaster.model.*;
import com.branegy.service.connection.api.ConnectionService;
import com.branegy.dbmaster.connection.ConnectionProvider;
import org.apache.commons.io.IOUtils

def source_server    = p_database.split("\\.")[0]
def source_database  = p_database.split("\\.")[1]

connectionSrv = dbm.getService(ConnectionService.class);
connectionInfo = connectionSrv.findByName(source_server)
connector = ConnectionProvider.getConnector(connectionInfo)

connection = connector.getJdbcConnection(source_database)
dbm.closeResourceOnExit(connection)

def sql = new Sql(connection)

logger.info("Query = ${p_query}")

def dialect = connector.connect()

def qSymbol = '\''

/*
switch (dialect.getDialectName().toLowerCase()) {
   case "oracle":
       qSymbol = '\''
   case "mysql":
       qSymbol = '\''
   case "nuodb":
       qSymbol = '\''
}
*/

def quote = {
   if (it == null) return it

   if (it.class == java.lang.String) return qSymbol + it.replaceAll('\'','\'\'')  + qSymbol;
   if (it.class == java.util.Date)       return qSymbol + it.replaceAll('"','\\\\"') + qSymbol;
   if (it.class == java.sql.Date)        return qSymbol + it.replaceAll('"','\\\\"') + qSymbol;
/*
   FOR ORACLE
   if (it.class == oracle.sql.TIMESTAMP) return 'TIMESTAMP ' + qSymbol + it + qSymbol;
   if (it.class == java.sql.Timestamp)   return 'TIMESTAMP ' + qSymbol + it + qSymbol;
*/
   if (it.class == java.math.BigDecimal) return it;
   if (it.class == java.sql.Timestamp)   return qSymbol + it + qSymbol;
   if (it.class == java.lang.Long)       return it;
   if (it.class == java.lang.Integer)    return it;
   if (it.class == java.lang.Boolean)    return it;
   if (it instanceof java.sql.Clob)        {
           Reader reader = it.getCharacterStream()
           String result = IOUtils.toString(reader)
           return qSymbol + result.replaceAll(qSymbol,'\\\\'+qSymbol) + qSymbol
   }
   logger.error("Unmapped Variable type "+it.class)
   return it
}


def colNames = []
def colType = [:]



// File file = new File(p_output_file);

// FileWriter writer = new FileWriter(file);
// BufferedWriter out = new BufferedWriter(writer, 1024 * 1024 * 10);

// write(records, bufferedWriter);
// bufferedWriter.close();


int counter = 0;

println "<pre>"
def insert_text = "insert into ${p_target_table} ( "

sql.eachRow(p_query,
       { meta2 -> for (i in 1..meta2.getColumnCount()) {
                    if (i>1) insert_text+=", "
                    insert_text+=meta2.getColumnName(i)
                    colNames << meta2.getColumnName(i)
                    colType[meta2.getColumnName(i)]= meta2.getColumnTypeName(i)
                }
        insert_text += ") values ("
       } ,

       { row ->  print insert_text
                colNames.eachWithIndex { it, i -> if (i>0) print ', '
                                                  print quote(row[ it ]).toString() }
                println ");"
                counter = counter + 1
                if (counter % 100==0) { logger.info ("Exported ${counter} rows") }
        }

)

logger.info ("Total rows exported - ${counter} ")

connection.close()

println "</pre>"

// out.close();
// writer.close();

logger.info("Data export completed")