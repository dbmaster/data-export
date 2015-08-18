import io.dbmaster.testng.BaseToolTestNGCase;

import static org.testng.Assert.assertTrue;
import org.testng.annotations.Test

import com.branegy.tools.api.ExportType;


public class DataExportIT extends BaseToolTestNGCase {

    @Test
    public void test() {
        def parameters = [ "p_database"     :  getTestProperty("data-export.p_database"),
                           "p_target_table" :  getTestProperty("data-export.p_target_table"),
                           "p_query"        :  getTestProperty("data-export.p_query")
             ]
        String result = tools.toolExecutor("data-export", parameters).execute()
        assertTrue(result.contains("insert into "), "Unexpected search results ${result}");
    }
}
