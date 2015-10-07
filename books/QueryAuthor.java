import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.json.JSONObject;
import org.json.JSONArray;

public class QueryAuthor {

    /*
     * TextArrayWritable is used to pass the book array between
     * mapper, combiner and reducer. get() -function inspired by
     * https://github.com/culturegraph/metafacture-cluster/blob/master/src/main/java/org/culturegraph/mf/cluster/util/TextArrayWritable.java
     */
    public final static class TextArrayWritable extends ArrayWritable {
        public TextArrayWritable() {
            super(Text.class);
        }

        public TextArrayWritable(Text[] values) {
            super(Text.class, values);
        }

        @Override
        public Text[] get() {
            final Writable[] writables = super.get();
            final Text[] texts = new Text[writables.length];

            for (int i=0; i<writables.length; i++) {
                texts[i] = (Text) writables[i];
            }

            return texts;
        }
    }

    /*
     *  Mapper class uses JSONObject to parse author and book from each
     *  input line. The author is written to the context as hadoop Text,
     *  and book as a TextArrayWritable. TextArrayWritable is used to
     *  keep the mapper output, combiner, and reducer input consistent.
     */
    private final static class QueryAuthorMapper
        extends Mapper<Object, Text, Text, TextArrayWritable> {

        @Override
        public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

            String authorQuery = context.getConfiguration().get("authorQuery");

            Text author = new Text();
            Text book   = new Text();

            JSONObject json;

            String line;
            StringTokenizer tokenizer = new StringTokenizer(value.toString(), "\n");

            while (tokenizer.hasMoreTokens()) {
                try {
                    line = tokenizer.nextToken();
                    json = new JSONObject(line);

                    author.set(json.getString("author"));

                    if (!authorQuery.equals(author.toString())) continue;

                    book.set(json.getString("book"));

                    Text[] singleBook = {book};
                    context.write(author, new TextArrayWritable(singleBook));

                } catch (Exception e) {} // ignore exceptions for now
            }
        }
    }

    /*
     * Combiner class is used to combine mapper output locally.
     * Note that the input values for reducer holds a list of
     * single length lists. Note that the input and output types are
     */
    private final static class QueryAuthorCombiner
        extends Reducer<Text, TextArrayWritable, Text, TextArrayWritable> {

        @Override
        public void reduce(Text key,
                           Iterable<TextArrayWritable> values,
                           Context context)
            throws IOException, InterruptedException {

            // Use ArrayList to combine books in values list
            ArrayList<Text> bookArray = new ArrayList<Text>();
            TextArrayWritable combinedBooks;

            for (TextArrayWritable singleBook : values) {
                // Assuming we have one book per value
                bookArray.add((singleBook.get())[0]);
            }

            // Create a TextArrayWritable from the bookArray.
            // note tht TextArrayWritable constructor expects
            // an Array instead of list.
            combinedBooks = new TextArrayWritable(
                                                  bookArray.toArray(new Text[bookArray.size()]));

            context.write(key, combinedBooks);
        }
    }

    /*
     * Reducer creates a JSONArray from the TextArrayWritable book lists,
     * and returns author, books json string as a key. The return values are
     * left NullWritable, as the whole json row is in the key string.
     */
    private final static class QueryAuthorReducer
        extends Reducer<Text, TextArrayWritable, Text, NullWritable> {

        @Override
        public void reduce(Text key, Iterable<TextArrayWritable> values, Context context)
            throws IOException, InterruptedException {

            JSONObject bookObj;
            JSONObject reducedObj;
            JSONArray booksArray = new JSONArray();

            for (TextArrayWritable books : values) {
                for (Text book : books.get()) {
                    bookObj = new JSONObject().put("book", book.toString());
                    booksArray.put(bookObj);
                }
            }

            // Note author comes after books to have them in more logical order
            reducedObj  = new JSONObject()
                .put("books", booksArray)
                .put("author", key.toString());

            context.write(new Text(reducedObj.toString()), NullWritable.get());
        }
    }

    /*
     * main function starts the hadoop system, using the mapper, combiner,
     * and reducer. First (other) argument is the name of input file and
     * second one the name of the output file.
     * The author query input is assumed to be at otherArgs[2], if it does
     * not contain whitespaces in between, and in otherArgs[2..otherArgs.length-1]
     * in case it contains whitespaces.
     * Not that if the query contains a sequence of multiple whitespaces,
     * they are interpreted as a single whitespace
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
            .getRemainingArgs();
        if (otherArgs.length < 3) {
            System.err.println("Usage: QueryAuthor <in> <out> <author>");
            System.exit(2);
        }

        String authorQuery = otherArgs[2];

        for (int i=3; i<otherArgs.length; i++) {
            authorQuery += " " + otherArgs[i];
        }

        conf.set("authorQuery", authorQuery);
        Job job = new Job(conf, "QueryAuthor");

        job.setJarByClass(QueryAuthor.class);

        job.setMapperClass(QueryAuthorMapper.class);
        job.setCombinerClass(QueryAuthorCombiner.class);
        job.setReducerClass(QueryAuthorReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TextArrayWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
