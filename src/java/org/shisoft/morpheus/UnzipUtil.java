package org.shisoft.morpheus;

import java.io.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Created by shisoft on 16-5-8.
 */
public class UnzipUtil {
    /**
     * Size of the buffer to read/write data
     */
    private static final int BUFFER_SIZE = 4096;
    /**
     * Extracts a zip file specified by the zipFilePath to a directory specified by
     * destDirectory (will be created if does not exists)
     * @param zipFilePath
     * @param destDirectory
     * @throws IOException
     */
    public static void unzip(String zipFilePath, String destDirectory) throws IOException {
        try
        {
            byte[] buf = new byte[1024];
            ZipInputStream zipinputstream = null;
            ZipEntry zipentry;
            zipinputstream = new ZipInputStream(
                    new FileInputStream(zipFilePath));

            zipentry = zipinputstream.getNextEntry();
            while (zipentry != null)
            {
                //for each entry to be extracted
                String entryName = zipentry.getName().replace("./", "/");
                int n;
                FileOutputStream fileoutputstream;
                File newFile = new File(entryName);
                String directory = newFile.getParent();

                if (directory == null)
                {
                    if(newFile.isDirectory())
                        break;
                }

                String fullPath = destDirectory + entryName;
                String parentDir = new File(fullPath).getParent();

                new File(parentDir).mkdirs();

                fileoutputstream = new FileOutputStream(fullPath);

                while ((n = zipinputstream.read(buf, 0, 1024)) > -1)
                    fileoutputstream.write(buf, 0, n);

                fileoutputstream.close();
                zipinputstream.closeEntry();
                zipentry = zipinputstream.getNextEntry();

            }//while

            zipinputstream.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }


    public static  void writeBytesToFile (byte [] myByteArray, String path) throws IOException {
        FileOutputStream fos = new FileOutputStream(path);
        fos.write(myByteArray);
        fos.close();
    }
}
