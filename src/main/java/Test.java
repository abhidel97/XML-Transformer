

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

public class Test
{
    private static final String INPUT_GZIP_FILE = "C:\\Users\\AR6A848N\\Desktop\\Spark-Scala\\enwiki-latest-abstract.xml.gz";
    private static final String OUTPUT_FILE = "C:\\Users\\AR6A848N\\Desktop\\Spark-Scala\\data.xml";

    public static void main( String[] args )
    {
        Test gZip = new Test();
        gZip.gunzipIt();
    }

    /**
     * GunZip it
     */
    public void gunzipIt(){

        byte[] buffer = new byte[1024];

        try{

            GZIPInputStream gzis =
                    new GZIPInputStream(new FileInputStream(INPUT_GZIP_FILE));

            FileOutputStream out =
                    new FileOutputStream(OUTPUT_FILE);

            int len;
            while ((len = gzis.read(buffer)) > 0) {
                out.write(buffer, 0, len);
            }

            gzis.close();
            out.close();

            System.out.println("Done");

        }catch(IOException ex){
            ex.printStackTrace();
        }
    }
}
