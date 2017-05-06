package org.hobbit.sparql_snb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import org.hobbit.core.components.AbstractSequencingTaskGenerator;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.hobbit.sparql_snb.util.SNBConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SNBTaskGenerator extends AbstractSequencingTaskGenerator {

	private static final Logger LOGGER = LoggerFactory.getLogger(SNBTaskGenerator.class);
	private int numberOfOperations;
	
	public SNBTaskGenerator() {
		
	}
	
    @Override
    public void init() throws Exception {
        LOGGER.info("Initialization begins.");
        super.init();
        
        internalInit();
        LOGGER.info("Initialization is over.");
    }
    
    private void internalInit() {
    	Map<String, String> env = System.getenv();
    	
    	// Number of operations
    	if (!env.containsKey(SNBConstants.GENERATOR_NUMBER_OF_OPERATIONS)) {
            LOGGER.error("Couldn't get \"" + SNBConstants.GENERATOR_NUMBER_OF_OPERATIONS + "\" from the properties. Aborting.");
            System.exit(1);
        }
    	numberOfOperations = Integer.parseInt(env.get(SNBConstants.GENERATOR_NUMBER_OF_OPERATIONS));	
	}

	@Override
	public void close() throws IOException {
		// Free the resources you requested here

		
        // Always close the super class after yours!
        super.close();
    }


	@Override
	protected void generateTask(byte[] data) throws Exception {
        String taskIdString = getNextTaskId();
        
        // Total number of operations achieved
        if (Integer.parseInt(taskIdString) >= numberOfOperations)
        	return;
        
        long timestamp = System.currentTimeMillis();
        
        String dataString = RabbitMQUtils.readString(data);
        String [] lines = dataString.split("\n");
        String queryText = prepareQueryText(lines[0]);
        data = RabbitMQUtils.writeString(queryText);
        sendTaskToSystemAdapter(taskIdString, data);

        data = RabbitMQUtils.writeString(dataString);
        sendTaskToEvalStorage(taskIdString, timestamp, data);
	}
	
    private String prepareQueryText(String text) throws Exception {
    	String [] parts = text.split("[{]");
    	String queryType = parts[0];
    	String [] arguments = parts[1].substring(0, parts[1].length()-1).split(", ");
    	String queryString = null;
    	if (queryType.startsWith("LdbcQuery"))
    		queryString = file2string(new File("snb_queries", "query" + queryType.replaceAll("[^0-9]*", "") + ".txt"));
    	else if (queryType.startsWith("LdbcUpdate"))
    		queryString = "INSERT DATA {\n" + prepareTriplets(queryType, arguments) + "}\n";
    	else
    		queryString = file2string(new File("snb_queries", "s" + queryType.replaceAll("[^0-9]*", "") + ".txt"));
    	for (String arg : arguments) {
			String [] tmp = arg.split("=");
			switch (tmp[0]) {
			case "personId":
				if (queryType.startsWith("LdbcQuery"))
					queryString = queryString.replaceAll("%" + tmp[0] + "%", String.format("%020d", Long.parseLong(tmp[1])));
				else
					queryString = queryString.replaceAll("%" + tmp[0] + "%", tmp[1]);
				break;
			case "maxDate":
			case "minDate":
			case "startDate":
				DateFormat format1 = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.ENGLISH);
				Date date = format1.parse(tmp[1]);
				DateFormat format2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'+00:00'");
				queryString = queryString.replaceAll("%" + tmp[0] + "%", format2.format(date));
				break;
			case "month":
				queryString = queryString.replaceAll("%month1%", tmp[1]);
				int nextMonth = Integer.parseInt(tmp[1]) + 1;
				if (nextMonth == 13)
				    nextMonth = 1;
				queryString = queryString.replaceAll("%month2%", String.valueOf(nextMonth));
				break;
			case "countryXName":
			case "countryYName":
				queryString = queryString.replaceAll("%" + tmp[0] + "%", tmp[1].substring(1, tmp[1].length()-1));
				break;
			default:
				queryString = queryString.replaceAll("%" + tmp[0] + "%", tmp[1]);
				break;
			}
		}

		return queryString;
	}
    
	private String prepareTriplets(String queryType, String[] arguments) throws ParseException {
		
		if (queryType.equals("LdbcUpdate1AddPerson")) {
			
		}
		else if (queryType.equals("LdbcUpdate2AddPostLike")) {
			long personId = 0, postId = 0;
			Date creationDate = null;
			for (String arg : arguments) {
				String [] tmp = arg.split("=");
				switch (tmp[0]) {
				case "personId":
					personId = Long.parseLong(tmp[1]);
					break;
				case "postId":
					postId = Long.parseLong(tmp[1]);
					break;
				case "creationDate":
					DateFormat format1 = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.ENGLISH);
					creationDate = format1.parse(tmp[1]);
					break;
				}
			}
			String personUri = "<http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers" + String.format("%020d", personId) + ">";
            String postUri = "<http://www.ldbc.eu/ldbc_socialnet/1.0/data/post" + String.format("%020d", postId) + ">";
            DateFormat df1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'+00:00'");
            df1.setTimeZone(TimeZone.getTimeZone("GMT"));
            String triplets [] = new String[1];
            triplets[0] = personUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/likes> [ <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/hasPost> " + postUri + "; <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/creationDate> \"" + df1.format(creationDate) + "\"^^xsd:dateTime ] .";
            return String.join("\n", triplets);
		}
		else if (queryType.equals("LdbcUpdate3AddCommentLike")) {
			long personId = 0, commentId = 0;
			Date creationDate = null;
			for (String arg : arguments) {
				String [] tmp = arg.split("=");
				switch (tmp[0]) {
				case "personId":
					personId = Long.parseLong(tmp[1]);
					break;
				case "commentId":
					commentId = Long.parseLong(tmp[1]);
					break;
				case "creationDate":
					DateFormat format1 = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.ENGLISH);
					creationDate = format1.parse(tmp[1]);
					break;
				}
			}
			String personUri = "<http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers" + String.format("%020d", personId) + ">";
            String commentUri = "<http://www.ldbc.eu/ldbc_socialnet/1.0/data/comm" + String.format("%020d", commentId) + ">";
            DateFormat df1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'+00:00'");
            df1.setTimeZone(TimeZone.getTimeZone("GMT"));
            String triplets [] = new String[1];
            triplets[0] = personUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/likes> [ <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/hasComment> " + commentUri + "; <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/creationDate> \"" + df1.format(creationDate) + "\"^^xsd:dateTime ] .";
            return String.join("\n", triplets);
		}
		else if (queryType.equals("LdbcUpdate4AddForum")) {
			
		}
		else if (queryType.equals("LdbcUpdate5AddForumMembership")) {
			long personId = 0, forumId = 0;
			Date joinDate = null;
			for (String arg : arguments) {
				String [] tmp = arg.split("=");
				switch (tmp[0]) {
				case "personId":
					personId = Long.parseLong(tmp[1]);
					break;
				case "forumId":
					forumId = Long.parseLong(tmp[1]);
					break;
				case "joinDate":
					DateFormat format1 = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.ENGLISH);
					joinDate = format1.parse(tmp[1]);
					break;
				}
			}
			String forumUri = "<http://www.ldbc.eu/ldbc_socialnet/1.0/data/forum" + String.format("%020d", forumId) + ">";
            String memberUri = "<http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers" + String.format("%020d", personId) + ">";
            DateFormat df1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'+00:00'");
            df1.setTimeZone(TimeZone.getTimeZone("GMT"));
            String triplets [] = new String[1];
            triplets[0] = forumUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/hasMember> [ <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/hasPerson> " + memberUri + "; <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/joinDate> \"" + df1.format(joinDate) + "\"] .";
            return String.join("\n", triplets);
		}
		else if (queryType.equals("LdbcUpdate6AddPost")) {
			
		}
		else if (queryType.equals("LdbcUpdate7AddComment")) {
			
		}
		else if (queryType.equals("LdbcUpdate8AddFriendship")) {
			long person1Id = 0, person2Id = 0;
			Date creationDate = null;
			for (String arg : arguments) {
				String [] tmp = arg.split("=");
				switch (tmp[0]) {
				case "person1Id":
					person1Id = Long.parseLong(tmp[1]);
					break;
				case "person2Id":
					person2Id = Long.parseLong(tmp[1]);
					break;
				case "creationDate":
					DateFormat format1 = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.ENGLISH);
					creationDate = format1.parse(tmp[1]);
					break;
				}
			}
			String person1Uri = "<http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers" + String.format("%020d", person1Id) + ">";
            String person2Uri = "<http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers" + String.format("%020d", person2Id) + ">";
            DateFormat df1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'+00:00'");
            df1.setTimeZone(TimeZone.getTimeZone("GMT"));
            String triplets [] = new String[4];
            triplets[0] = person1Uri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/knows> [ <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/hasPerson> " + person2Uri + "; <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/creationDate> \"" + df1.format(creationDate) + "\"^^xsd:dateTime ] .";
            triplets[1] = person2Uri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/knows> [ <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/hasPerson> " + person1Uri + "; <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/creationDate> \"" + df1.format(creationDate) + "\"^^xsd:dateTime ] .";
            triplets[2] = person1Uri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/knows> " + person2Uri + " .";
            triplets[3] = person2Uri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/knows> " + person1Uri + " .";
            return String.join("\n", triplets);
		}
		return "";
	}

	private String file2string(File file) throws Exception {
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(file));
			StringBuffer sb = new StringBuffer();

			while (true) {
				String line = reader.readLine();
				if (line == null)
					break;
				else {
					sb.append(line);
					sb.append("\n");
				}
			}
			return sb.toString();
		} catch (IOException e) {
			throw new Exception("Error openening or reading file: " + file.getAbsolutePath(), e);
		} finally {
			try {
				if (reader != null)
					reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
