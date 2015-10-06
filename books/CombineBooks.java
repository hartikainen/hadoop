import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

//TODO import necessary components

/*
 *  Modify this file to combine books from the same other into
 *  single JSON object.
 *  i.e. {"author": "Tobias Wells", "books": [{"book":"A die in the country"},{"book": "Dinky died"}]}
 *  Beaware that, this may work on anynumber of nodes!
 *
 */

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

            while (tokenizer.hasMoreTokens) {
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
