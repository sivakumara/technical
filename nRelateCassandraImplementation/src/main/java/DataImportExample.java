
/**
 * Created with IntelliJ IDEA.
 * User: sivakumar
 * Date: 22/10/13
 * Time: 4:32 PM
 * To change this template use File | Settings | File Templates.
 */
/**
 * Disclaimer:
 * This file is an example on how to use the Cassandra SSTableSimpleUnsortedWriter class to create
 * sstables from a csv input file.
 * While this has been tested to work, this program is provided "as is" with no guarantee. Moreover,
 * it's primary aim is toward simplicity rather than completness. In partical, don't use this as an
 * example to parse csv files at home.
 */
import java.nio.ByteBuffer;
import java.io.*;
import java.util.UUID;

import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.SSTableSimpleUnsortedWriter;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.apache.cassandra.utils.UUIDGen.decompose;

public class DataImportExample
{
    static String filename;

    public static void main(String[] args) throws IOException
    {
        filename = args[0];
        BufferedReader reader = new BufferedReader(new FileReader(filename));

        String keyspace = "Demo";
        File directory = new File(keyspace);
        if (!directory.exists())
            directory.mkdir();

        IPartitioner  partitioner = new Murmur3Partitioner();
        SSTableSimpleUnsortedWriter usersWriter = new SSTableSimpleUnsortedWriter(directory,partitioner,keyspace,"Users",AsciiType.instance,null,64);

        SSTableSimpleUnsortedWriter loginWriter = new SSTableSimpleUnsortedWriter(
                directory,   partitioner,
                keyspace,
                "Logins",
                AsciiType.instance,
                null,
                64);

        String line;
        int lineNumber = 1;
        CsvEntry entry = new CsvEntry();
        // There is no reason not to use the same timestamp for every column in that example.
        long timestamp = System.currentTimeMillis() * 1000;
        while ((line = reader.readLine()) != null)
        {
            if (entry.parse(line, lineNumber))
            {
                ByteBuffer uuid = ByteBuffer.wrap(decompose(entry.key));
                usersWriter.newRow(uuid);
                usersWriter.addColumn(bytes("firstname"), bytes(entry.firstname), timestamp);
                usersWriter.addColumn(bytes("lastname"), bytes(entry.lastname), timestamp);
                usersWriter.addColumn(bytes("password"), bytes(entry.password), timestamp);
                usersWriter.addColumn(bytes("age"), bytes(entry.age), timestamp);
                usersWriter.addColumn(bytes("email"), bytes(entry.email), timestamp);

            }
            lineNumber++;
        }
        // Don't forget to close!
        usersWriter.close();
        loginWriter.close();
        System.out.println("SSTable Created Successfully");
        System.exit(0);
    }

    static class CsvEntry
    {
        UUID key;
        String firstname;
        String lastname;
        String password;
        long age;
        String email;

        boolean parse(String line, int lineNumber)
        {
            // Ghetto csv parsing
            String[] columns = line.split(",");
            if (columns.length != 6)
            {
                System.out.println(String.format("Invalid input '%s' at line %d of %s", line, lineNumber, filename));
                return false;
            }
            try
            {
                key = UUID.fromString(columns[0].trim());
                firstname = columns[1].trim();
                lastname = columns[2].trim();
                password = columns[3].trim();
                age = Long.parseLong(columns[4].trim());
                email = columns[5].trim();
                return true;
            }
            catch (NumberFormatException e)
            {
                System.out.println(String.format("Invalid number in input '%s' at line %d of %s", line, lineNumber, filename));
                return false;
            }
        }
    }
}

