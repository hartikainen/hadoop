import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

import org.codehaus.jackson;

//TODO import necessary components

/*
 *  Modify this file to combine books from the same other into
 *  single JSON object.
 *  i.e. {"author": "Tobias Wells", "books": [{"book":"A die in the country"},{"book": "Dinky died"}]}
 *  Beaware that, this may work on anynumber of nodes!
 *
 */

// JSONObject obj = new JSONObject(str);
// JSONArray arr = new JSONArray(obj.get("subcategories"));

// for(int i = 0; i < arr.length; i++)
// JSONObject temp = arr.getJSONObject(i);
// Category c = new Category();
// c.setId(temp.get("id"));

public class CombineBooks {

    private final static class TextArrayWritable extends ArrayWritable {
        public TextArrayWritable() {
            super(Text.class);
        }
    }

    private final static class CombineBooksMapper
        extends Mapper<Object, Text, Text, Text> {

        private final JsonFactory factory = new JsonFactory();

        public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

            Text author = new Text();
            Text book   = new Text();
            JsonParser parser;
            String line;
            StringTokenizer tokenizer = new StringTokenizer(value.toString(), "\n");

            while (tokenizer.hasMoreTokens()) {
                try {
                    line = tokenizer.nextToken();
                    parser = factory.createJsonParser(line);

                    parser.nextToken();
                    author.set(parser.getText());

                    parser.nextToken();
                    book.set(parser.getText());

                    context.write(author, book);

                } catch (Exception e) {} // ignore exceptions for now
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
            .getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: CombineBooks <in> <out>");
            System.exit(2);
        }

        //TODO implement CombineBooks

        Job job = new Job(conf, "CombineBooks");

        //TODO implement CombineBooks

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
