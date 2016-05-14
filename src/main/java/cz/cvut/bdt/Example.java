package cz.cvut.bdt;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.ByteBuffer;
import java.util.*;

import org.hbase.async.Config;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;

final class Example {

	static final String configFile = "asynchbase.properties";
	private static final java.lang.String WHITESPACE_PATTERN = "\\s+";
	private static final String CHAR_TO_EXIT = "X";
	private static HashMap<String, String> vocabulary;
	private static HashMap<String, Long> numWordsInDoc;
	private static int N;
	private static long avdl;
    public static final double K = 1.75; // <1.2; 2.0>
    public static final double B = 0.75;
    public static final int PRINT_DOCS_NUM = 10;
    public static final String WIKI_ARTICLE_URL_PREFIX = "https://en.wikipedia.org/?curid=";

	public static void main(final String[] args) throws Exception {

		//-- PARSE ARGUMENTS --//
		ArgumentParser parser = new ArgumentParser("HBaseClient");
		parser.addArgument("async_conf", true, true, "specify path to asynchbase.properties");
		parser.addArgument("auth_conf", true, true, "specify path to java.security.auth.login.config");
		parser.addArgument("table", true, true, "specify table name");
		parser.addArgument("vocab", true, true, "specify vocabulary");
		parser.addArgument("docinfo", true, true, "specify docinfo");
		parser.parseAndCheck(args);
		String async_conf = parser.getString("async_conf");
		String auth_conf = parser.getString("auth_conf");
		String table = parser.getString("table");
		String vocab = parser.getString("vocab");
		String docinfo = parser.getString("docinfo");

		//-- SET java.security.auth.login.config --//
		System.setProperty("java.security.auth.login.config",auth_conf);

		readVocab(vocab);
		readDocInfo(docinfo);

		//-- LOAD DATA FROM HBASE --//
		Config config = new Config(async_conf);
		HBaseClient client = new HBaseClient(config);

		String queryStr;
		while ((queryStr = readQuery()) != null) {
			query(table, client, queryStr);
		}

		System.out.println("Bye!");
	}

	private static String readQuery() {
		Scanner scanner = new Scanner(System.in);
		System.out.print("Enter query (words space separated) or "+CHAR_TO_EXIT+" to exit: ");
		String readLine = scanner.next();

		if(CHAR_TO_EXIT.equals(readLine.trim().toUpperCase())) {
			return null;
		}
		return readLine;
	}

	private static void query(String table, HBaseClient client, String query) {
		String [] query_terms = query.split(WHITESPACE_PATTERN);

        HashMap<String, Integer> doc_ids_tfs = new HashMap<>();
        HashMap<String, Integer> n_query = new HashMap<>();

		for (String query_term : query_terms) {
			String row_key = vocabulary.get(query_term);
			GetRequest get = new GetRequest(table, row_key);

			try {
				ArrayList<KeyValue> result = client.get(get).joinUninterruptibly();
//				System.out.println("Results for key " + row_key + " in table " + table);

                n_query.put(query_term, result.size());

				for (KeyValue res : result) {

					//load doc_id  (string)
					String doc_id = new String(res.qualifier());    //	StandardCharsets.UTF_8

					// load tf (integer)
					ByteBuffer bb = ByteBuffer.wrap(res.value());
					int tf = bb.getInt();

					// print
//					System.out.println(doc_id + " : " + tf);

					doc_ids_tfs.put(doc_id, tf);
				}

			} catch (Exception e) {
				System.out.println("Get failed:");
				e.printStackTrace();
			}
		}

		client.shutdown();

        List<Map.Entry<String, Double>> scores = countBM25(doc_ids_tfs, query_terms, n_query);
        printSearchHits(scores);
	}

    private static void printSearchHits(List<Map.Entry<String, Double>> scores) {

        int printedCnt = 0;
        for(Map.Entry<String, Double> entry : scores) {
            String doc_id = entry.getKey();

            String url = WIKI_ARTICLE_URL_PREFIX + doc_id;
            System.out.println("[" + (printedCnt + 1) + ".]\t" + url);

            if(printedCnt++ >= PRINT_DOCS_NUM) {
                break;
            }
        }
    }

    private static List<Map.Entry<String, Double>> countBM25(HashMap<String, Integer> doc_ids_tfs, String [] query_terms, HashMap<String, Integer> n_query) {

        HashMap<String, Double> scores = new HashMap<>();

        // score (D, Q):
        for (String doc_id : doc_ids_tfs.keySet()) {
            long tf = doc_ids_tfs.get(doc_id);
            double score = 0;

            for (String query_term : query_terms) {
                double idf = getIDF(query_term, n_query);
                score += idf * (tf  * (K + 1) / ( tf + K * (1 - B + B * (numWordsInDoc.get(doc_id) / avdl)) ) );
            }
            scores.put(doc_id, score);
        }

        List<Map.Entry<String,Double>> aList = new LinkedList<>(scores.entrySet());
        Collections.sort(aList, new Comparator<Map.Entry<String, Double>>() {
            @Override
            public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) {
                return o1.getValue().compareTo(o2.getValue());
            }
        });
        Collections.reverse(aList);
        return aList;
    }

    private static double getIDF(String query_term, HashMap<String, Integer> n_query) {
        return Math.log( (N - n_query.get(query_term) + 0.5 ) / (n_query.get(query_term) + 0.5) );
    }

    private static void readDocInfo(String filename) {

        numWordsInDoc = new HashMap<>();
		try
		{
			BufferedReader reader = new BufferedReader(new FileReader(filename));
			String line;
			long globalNumWords = 0;
			while ((line = reader.readLine()) != null)
			{
				String [] row = line.split(WHITESPACE_PATTERN);
				String doc_id = row[0];
				long num_words = Long.parseLong(row[0]);
				globalNumWords += num_words;

				numWordsInDoc.put(doc_id, num_words);
			}

			// prumerna delka dokumentu v poctu slov
			avdl = globalNumWords / N;

			reader.close();
		}
		catch (Exception e)
		{
			System.err.format("Exception occurred trying to read '%s'.", filename);
			e.printStackTrace();
		}
	}

	private static void readVocab(String filename) {

		vocabulary = new HashMap<>();
		try
		{
			BufferedReader reader = new BufferedReader(new FileReader(filename));
			String line;
			// read first line (=N)
			N = Integer.parseInt(reader.readLine().split(WHITESPACE_PATTERN)[1]);
			int word_id = 1;  // vocabulary zacina word_id == 1
			while ((line = reader.readLine()) != null)
			{
				String [] row = line.split(WHITESPACE_PATTERN);
				vocabulary.put(row[0], word_id++ + "");
			}
			reader.close();
		}
		catch (Exception e)
		{
			System.err.format("Exception occurred trying to read '%s'.", filename);
			e.printStackTrace();
		}
	}

}