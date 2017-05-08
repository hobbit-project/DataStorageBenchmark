package org.hobbit.sparql_snb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.hobbit.core.components.AbstractSequencingTaskGenerator;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.hobbit.sparql_snb.util.SNBConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SNBTaskGenerator extends AbstractSequencingTaskGenerator {

	private static final Logger LOGGER = LoggerFactory.getLogger(SNBTaskGenerator.class);
	private int numberOfOperations;
	
	Pattern personIdPattern = Pattern.compile("personId=([^,]+)");
	Pattern person1IdPattern = Pattern.compile("person1Id=([^,]+)");
	Pattern person2IdPattern = Pattern.compile("person2Id=([^,]+)");
	Pattern personFirstNamePattern = Pattern.compile("personFirstName='([^']*)'");
	Pattern personLastNamePattern = Pattern.compile("personLastName='([^']*)'");
	Pattern genderPattern = Pattern.compile("gender='([^']+)'");
	Pattern birthdayPattern = Pattern.compile("birthday=([^,]*)");
	Pattern locationIpPattern = Pattern.compile("locationIp='([^']+)'");
	Pattern browserUsedPattern = Pattern.compile("browserUsed='([^']+)'");
	Pattern cityIdPattern = Pattern.compile("cityId=([^,]*)");
	Pattern languagesPattern = Pattern.compile("languages=\\[([^\\]]*)\\]");
	Pattern emailsPattern = Pattern.compile("emails=\\[([^\\]]*)\\]");
	Pattern tagIdsPattern = Pattern.compile("tagIds=\\[([^\\]]*)\\]");
	Pattern studyAtPattern = Pattern.compile("studyAt=\\[([^\\]]*)\\]");
	Pattern workAtPattern = Pattern.compile("workAt=\\[([^\\]]*)\\]");
	Pattern organizationIdPattern = Pattern.compile("organizationId=([0-9]+)");
	Pattern yearPattern = Pattern.compile("year=([0-9]+)");
	Pattern postIdPattern = Pattern.compile("postId=([^,]+)");
	Pattern commentIdPattern = Pattern.compile("commentId=([^,]+)");
	Pattern forumIdPattern = Pattern.compile("forumId=([^,]+)");
	Pattern creationDatePattern = Pattern.compile("creationDate=([^,]+)");
	Pattern joinDatePattern = Pattern.compile("joinDate=([^,]+)");
	Pattern forumTitlePattern = Pattern.compile("forumTitle='([^']*)'");
	Pattern moderatorPersonIdPattern = Pattern.compile("moderatorPersonId=([^,]+)");
	Pattern imageFilePattern = Pattern.compile("imageFile='([^']*)'");
	Pattern languagePattern = Pattern.compile("language='([^']*)'");
	Pattern contentPattern = Pattern.compile("content='([^']*)'");
	Pattern lengthPattern = Pattern.compile("length=([^,]+)");
	Pattern authorPersonIdPattern = Pattern.compile("authorPersonId=([^,]+)");
	Pattern countryIdPattern = Pattern.compile("countryId=([^,]+)");
	Pattern mentionedIdsPattern = Pattern.compile("mentionedIds=\\[([^\\]]*)\\]");
	Pattern privacyPattern = Pattern.compile("privacy=([^,]*)");
	Pattern linkPattern = Pattern.compile("link=([^,]*)");
	Pattern replyToPostIdPattern = Pattern.compile("replyToPostId=([^,]+)");
	Pattern replyToCommentIdPattern = Pattern.compile("replyToCommentId=([^,]+)");
	Pattern gifPattern = Pattern.compile("gif=([^,]*)");
	
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
    	String [] parts = text.split("[{]", 2);
    	String queryType = parts[0];
    	String [] arguments = parts[1].substring(0, parts[1].length()-1).split(", ");
    	String queryString = null;
    	if (queryType.startsWith("LdbcUpdate")) {
    		queryString = preparePrefixes();
    		queryString += "INSERT DATA { GRAPH <https://github.com/hobbit-project/sparql-snb> {\n" + prepareTriplets(queryType, parts[1].substring(0, parts[1].length()-1)) + "\n}\n}\n";
    	}
    	else {
    		if (queryType.startsWith("LdbcQuery")) {
    			queryString = file2string(new File("snb_queries", "query" + queryType.replaceAll("[^0-9]*", "") + ".txt"));
    		}
    		else {
    			queryString = file2string(new File("snb_queries", "s" + queryType.replaceAll("[^0-9]*", "") + ".txt"));
    		}
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
    	}

		return queryString;
	}
    
	private String preparePrefixes() {
		return "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n";
	}

	private String prepareTriplets(String queryType, String arguments) throws UnsupportedEncodingException, ParseException {
		if (queryType.equals("LdbcUpdate1AddPerson")) {
			long personId = Long.parseLong(extractWord(arguments, personIdPattern));
			String personFirstName = extractWord(arguments, personFirstNamePattern);
			String personLastName = extractWord(arguments, personLastNamePattern);
			String gender = extractWord(arguments, genderPattern);
			DateFormat format1 = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.ENGLISH);
			Date birthday = null;
			try {
				birthday = format1.parse(extractWord(arguments, birthdayPattern));
			} catch (ParseException e) {
				//NOTHING
			}
			Date creationDate = format1.parse(extractWord(arguments, creationDatePattern));
			String locationIp = extractWord(arguments, locationIpPattern);
			String browserUsed = extractWord(arguments, browserUsedPattern);
			long cityId = Long.parseLong(extractWord(arguments, cityIdPattern));
			List<String> languages = new ArrayList<String>(Arrays.asList(extractWord(arguments, languagesPattern).split(", ")));
			if (languages.size() == 1 && languages.get(0).equals(""))
				languages.clear();
			List<String> emails = new ArrayList<String>(Arrays.asList(extractWord(arguments, emailsPattern).split(", ")));
			if (emails.size() == 1 && emails.get(0).equals(""))
				emails.clear();
			List<String> tagIds = new ArrayList<String>(Arrays.asList(extractWord(arguments, tagIdsPattern).split(", ")));
			if (tagIds.size() == 1 && tagIds.get(0).equals(""))
				tagIds.clear();
			List<String> universities = new ArrayList<String>(Arrays.asList(extractWord(arguments, studyAtPattern).split("\\}, ")));
			if (universities.size() == 1 && universities.get(0).equals(""))
				universities.clear();
			List<Integer> studyAtOrgIds = new ArrayList<Integer>();
			List<Integer> studyAtYears = new ArrayList<Integer>();
				for (String u : universities) {
					studyAtOrgIds.add(Integer.parseInt(extractWord(u, organizationIdPattern)));
					studyAtYears.add(Integer.parseInt(extractWord(u, yearPattern)));
				}
			List<String> companies = new ArrayList<String>(Arrays.asList(extractWord(arguments, workAtPattern).split("\\}, ")));
			if (companies.size() == 1 && companies.get(0).equals(""))
				companies.clear();
			List<Long> workAtOrgIds = new ArrayList<Long>();
			List<Long> workAtYears = new ArrayList<Long>();
				for (String c : companies) {
					workAtOrgIds.add(Long.parseLong(extractWord(c, organizationIdPattern)));
					workAtYears.add(Long.parseLong(extractWord(c, yearPattern))); 
				}
			
			String personUri = "<http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers" + String.format("%020d", personId) + ">";
            DateFormat df1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'+00:00'");
            DateFormat df2 = new SimpleDateFormat("yyyy-MM-dd");
            df1.setTimeZone(TimeZone.getTimeZone("GMT"));
            df2.setTimeZone(TimeZone.getTimeZone("GMT"));
            
			List<String> triplets = new ArrayList<String>();
			triplets.add(personUri + " a <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/Person> .");
            if (!personFirstName.equals(""))
                triplets.add(personUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/firstName> \"" + new String(personFirstName.getBytes("UTF-8"), "ISO-8859-1") + "\" .");
            if (!personLastName.equals(""))
                triplets.add(personUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/lastName> \"" + new String(personLastName.getBytes("UTF-8"), "ISO-8859-1") + "\" .");
            
            triplets.add(personUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/gender> \"" + gender + "\" .");
            if (birthday != null)
                triplets.add(personUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/birthday> \"" + df2.format(birthday) + "\"^^xsd:date .");
            triplets.add(personUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/creationDate> \"" + df1.format(creationDate) + "\"^^xsd:dateTime .");
            triplets.add(personUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/locationIP> \"" + locationIp + "\" .");
            triplets.add(personUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/browserUsed> \"" + browserUsed + "\" .");
            triplets.add(personUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/isLocatedIn> <" + placeUri(cityId) + "> .");
            triplets.add(personUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/id> \"" + personId + "\"^^xsd:long .");
            for (int k = 0; k < languages.size(); k++)
            	triplets.add(personUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/speaks> \"" + languages.get(k) + "\" .");
            for (int k = 0; k < emails.size(); k++)
                    triplets.add(personUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/email> \"" + emails.get(k) + "\" .");
            for (int k = 0; k < tagIds.size(); k++)
                triplets.add(personUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/hasInterest> <" + tagUri(tagIds.get(k)) + "> .");
            for (int k = 0; k < studyAtOrgIds.size(); k++)
                triplets.add(personUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/studyAt> [ <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/hasOrganisation> <" + universityUri(studyAtOrgIds.get(k)) + ">; <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/classYear> \"" + studyAtYears.get(k) + "\"] .");
            for (int k = 0; k < workAtOrgIds.size(); k++)
                triplets.add(personUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/workAt> [ <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/hasOrganisation> <" + companyUri(workAtOrgIds.get(k)) + ">; <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/workFrom> \"" + workAtYears.get(k) + "\"] .");
            return String.join("\n", triplets);
		}
		else if (queryType.equals("LdbcUpdate2AddPostLike")) {
			long personId = Long.parseLong(extractWord(arguments, personIdPattern));
			long postId = Long.parseLong(extractWord(arguments, postIdPattern));
			DateFormat format1 = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.ENGLISH);
			Date creationDate = format1.parse(extractWord(arguments, creationDatePattern));
			String personUri = "<http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers" + String.format("%020d", personId) + ">";
            String postUri = "<http://www.ldbc.eu/ldbc_socialnet/1.0/data/post" + String.format("%020d", postId) + ">";
            DateFormat df1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'+00:00'");
            df1.setTimeZone(TimeZone.getTimeZone("GMT"));
            String triplets [] = new String[1];
            triplets[0] = personUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/likes> [ <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/hasPost> " + postUri + "; <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/creationDate> \"" + df1.format(creationDate) + "\"^^xsd:dateTime ] .";
            return String.join("\n", triplets);
		}
		else if (queryType.equals("LdbcUpdate3AddCommentLike")) {
			long personId = Long.parseLong(extractWord(arguments, personIdPattern));
			long commentId = Long.parseLong(extractWord(arguments, commentIdPattern));
			DateFormat format1 = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.ENGLISH);
			Date creationDate = format1.parse(extractWord(arguments, creationDatePattern));
			String personUri = "<http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers" + String.format("%020d", personId) + ">";
            String commentUri = "<http://www.ldbc.eu/ldbc_socialnet/1.0/data/comm" + String.format("%020d", commentId) + ">";
            DateFormat df1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'+00:00'");
            df1.setTimeZone(TimeZone.getTimeZone("GMT"));
            String triplets [] = new String[1];
            triplets[0] = personUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/likes> [ <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/hasComment> " + commentUri + "; <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/creationDate> \"" + df1.format(creationDate) + "\"^^xsd:dateTime ] .";
            return String.join("\n", triplets);
		}
		else if (queryType.equals("LdbcUpdate4AddForum")) {
			long forumId = Long.parseLong(extractWord(arguments, forumIdPattern));
			String forumTitle = extractWord(arguments, forumTitlePattern);
			DateFormat format1 = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.ENGLISH);
			Date creationDate = format1.parse(extractWord(arguments, creationDatePattern));
			long moderatorPersonId = Long.parseLong(extractWord(arguments, moderatorPersonIdPattern));
			List<String> tagIds = new ArrayList<String>(Arrays.asList(extractWord(arguments, tagIdsPattern).split(", ")));
			if (tagIds.size() == 1 && tagIds.get(0).equals(""))
				tagIds.clear();
			
			String forumUri = "<http://www.ldbc.eu/ldbc_socialnet/1.0/data/forum" + String.format("%020d", forumId) + ">";
			String moderatorUri = "<http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers" + String.format("%020d", moderatorPersonId) + ">";
			DateFormat df1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'+00:00'");
			df1.setTimeZone(TimeZone.getTimeZone("GMT"));
			String triplets [] = new String[5 + tagIds.size()];
			triplets[0] = forumUri + " a <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/Forum> .";
			triplets[1] = forumUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/title> \"" + new String(forumTitle.getBytes("UTF-8"), "ISO-8859-1") + "\" .";
			triplets[2] = forumUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/creationDate> \"" + df1.format(creationDate) + "\"^^xsd:dateTime .";
			triplets[3] = forumUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/hasModerator> " + moderatorUri + " .";
			triplets[4] = forumUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/id> \"" + forumId + "\"^^xsd:long . ";
			for (int k = 0; k < tagIds.size(); k++)
				triplets[5 + k] = forumUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/hasTag> <" + tagUri(tagIds.get(k)) + "> .";
			return String.join("\n", triplets);
		}
		else if (queryType.equals("LdbcUpdate5AddForumMembership")) {
			long personId = Long.parseLong(extractWord(arguments, personIdPattern));
			long forumId = Long.parseLong(extractWord(arguments, forumIdPattern));
			DateFormat format1 = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.ENGLISH);
			Date joinDate = format1.parse(extractWord(arguments, joinDatePattern));
			String forumUri = "<http://www.ldbc.eu/ldbc_socialnet/1.0/data/forum" + String.format("%020d", forumId) + ">";
            String memberUri = "<http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers" + String.format("%020d", personId) + ">";
            DateFormat df1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'+00:00'");
            df1.setTimeZone(TimeZone.getTimeZone("GMT"));
            String triplets [] = new String[1];
            triplets[0] = forumUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/hasMember> [ <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/hasPerson> " + memberUri + "; <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/joinDate> \"" + df1.format(joinDate) + "\"] .";
            return String.join("\n", triplets);
		}
		else if (queryType.equals("LdbcUpdate6AddPost")) {
			long postId = Long.parseLong(extractWord(arguments, postIdPattern));
			String imageFile = extractWord(arguments, imageFilePattern);
			DateFormat format1 = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.ENGLISH);
			Date creationDate = format1.parse(extractWord(arguments, creationDatePattern));
			String locationIp = extractWord(arguments, locationIpPattern);
			String browserUsed = extractWord(arguments, browserUsedPattern);
			String language = extractWord(arguments, languagePattern);
			String content = extractWord(arguments, contentPattern);
			int length = Integer.parseInt(extractWord(arguments, lengthPattern));
			long authorPersonId = Long.parseLong(extractWord(arguments, authorPersonIdPattern));
			long forumId = Long.parseLong(extractWord(arguments, forumIdPattern));
			Long countryId = Long.parseLong(extractWord(arguments, countryIdPattern));
			List<String> tagIds = new ArrayList<String>(Arrays.asList(extractWord(arguments, tagIdsPattern).split(", ")));
			if (tagIds.size() == 1 && tagIds.get(0).equals(""))
				tagIds.clear();
			List<String> mentionedIds = new ArrayList<String>(Arrays.asList(extractWord(arguments, mentionedIdsPattern).split(", ")));
			if (mentionedIds.size() == 1 && mentionedIds.get(0).equals(""))
				mentionedIds.clear();
			String privacy = extractWord(arguments, privacyPattern);
			String link = extractWord(arguments, linkPattern);

            String postUri = "<http://www.ldbc.eu/ldbc_socialnet/1.0/data/post" + String.format("%020d", postId) + ">";
            String forumUri = "<http://www.ldbc.eu/ldbc_socialnet/1.0/data/forum" + String.format("%020d", forumId) + ">";
            String authorUri = "<http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers" + String.format("%020d", authorPersonId) + ">";
            DateFormat df1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'+00:00'");
            df1.setTimeZone(TimeZone.getTimeZone("GMT"));

            List<String> triplets = new ArrayList<String>();
            triplets.add(postUri + " a <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/Post> .");
            triplets.add(postUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/locationIP> \"" + locationIp + "\" .");
            triplets.add(postUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/creationDate> \"" + df1.format(creationDate) + "\"^^xsd:dateTime .");
            triplets.add(postUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/browserUsed> \"" + browserUsed + "\" .");
            if (imageFile.equals("")) {
            	triplets.add(postUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/language> \"" + language + "\" .");
            	triplets.add(postUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/content> \"" + new String(content.getBytes("UTF-8"), "ISO-8859-1") + "\" .");
            	triplets.add(postUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/length> \"" + length + "\" .");
            }
            else {
            	triplets.add(postUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/imageFile> \"" + imageFile + "\" .");
            }
            triplets.add(postUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/hasCreator> " + authorUri + " .");
            triplets.add(postUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/id> \"" + postId + "\"^^xsd:long .");
            triplets.add(forumUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/containerOf> " + postUri + " .");
            if (countryId >= 0)
                triplets.add(postUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/isLocatedIn> <" + placeUri(countryId) + "> .");
            for (int k = 0; k < tagIds.size(); k++)
                triplets.add(postUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/hasTag> <" + tagUri(tagIds.get(k)) + "> .");
            for (int l = 0; l < mentionedIds.size(); l++)
                triplets.add(postUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/mentions> <http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers" + String.format("%020d", Long.parseLong(mentionedIds.get(l))) + "> .");
            if (privacy != null && !privacy.equals(""))
                triplets.add(postUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/visible> \"" + privacy + "\"^^xsd:boolean .");
            if (!link.equals(""))
                triplets.add(postUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/links> \"" + link + "\" .");
            return String.join("\n", triplets);
            
		}
		else if (queryType.equals("LdbcUpdate7AddComment")) {
			long commentId = Long.parseLong(extractWord(arguments, commentIdPattern));
			DateFormat format1 = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.ENGLISH);
			Date creationDate = format1.parse(extractWord(arguments, creationDatePattern));
			String locationIp = extractWord(arguments, locationIpPattern);
			String browserUsed = extractWord(arguments, browserUsedPattern);
			String content = extractWord(arguments, contentPattern);
			int length = Integer.parseInt(extractWord(arguments, lengthPattern));
			long authorPersonId = Long.parseLong(extractWord(arguments, authorPersonIdPattern));
			Long countryId = Long.parseLong(extractWord(arguments, countryIdPattern));
			Long replyToPostId = Long.parseLong(extractWord(arguments, replyToPostIdPattern));
			Long replyToCommentId = Long.parseLong(extractWord(arguments, replyToCommentIdPattern));
			List<String> tagIds = new ArrayList<String>(Arrays.asList(extractWord(arguments, tagIdsPattern).split(", ")));
			if (tagIds.size() == 1 && tagIds.get(0).equals(""))
				tagIds.clear();
			List<String> mentionedIds = new ArrayList<String>(Arrays.asList(extractWord(arguments, mentionedIdsPattern).split(", ")));
			if (mentionedIds.size() == 1 && mentionedIds.get(0).equals(""))
				mentionedIds.clear();
			String privacy = extractWord(arguments, privacyPattern);
			String link = extractWord(arguments, linkPattern);
			String gif = extractWord(arguments, gifPattern);
			
			String commentUri = "<http://www.ldbc.eu/ldbc_socialnet/1.0/data/comm" + String.format("%020d", commentId) + ">";
            String authorUri = "<http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers" + String.format("%020d", authorPersonId) + ">";
            String postUri = null;
            DateFormat df1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'+00:00'");
            df1.setTimeZone(TimeZone.getTimeZone("GMT"));
            
			List<String> triplets = new ArrayList<String>();
			triplets.add(commentUri + " a <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/Comment> .");
            triplets.add(commentUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/locationIP> \"" + locationIp + "\" .");
            triplets.add(commentUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/creationDate> \"" + df1.format(creationDate) + "\"^^xsd:dateTime .");
            triplets.add(commentUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/browserUsed> \"" + browserUsed + "\" .");
            if (!content.equals("")) {
                triplets.add(commentUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/content> \"" + new String(content.getBytes("UTF-8"), "ISO-8859-1") + "\" .");
                triplets.add(commentUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/length> \"" + length + "\" .");
            }
            triplets.add(commentUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/hasCreator> " + authorUri + " .");
            if (replyToPostId == -1)
                    postUri = "<http://www.ldbc.eu/ldbc_socialnet/1.0/data/comm" + String.format("%020d", replyToCommentId) + ">";
            else
                    postUri = "<http://www.ldbc.eu/ldbc_socialnet/1.0/data/post" + String.format("%020d", replyToPostId) + ">";
            triplets.add(commentUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/replyOf> " + postUri + " .");
            triplets.add(commentUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/id> \"" + commentId + "\"^^xsd:long .");
            if (countryId >= 0)
                triplets.add(commentUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/isLocatedIn> <" + placeUri(countryId) + "> .");
            for (int k = 0; k < tagIds.size(); k++)
                triplets.add(commentUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/hasTag> <" + tagUri(tagIds.get(k)) + "> .");
            for (int l = 0; l < mentionedIds.size(); l++)
                triplets.add(commentUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/mentions> <http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers" + String.format("%020d", Long.parseLong(mentionedIds.get(l))) + "> .");
            if (privacy != null  && !privacy.equals(""))
                triplets.add(commentUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/visible> \"" + privacy + "\"^^xsd:boolean .");
            if (!link.equals(""))
                triplets.add(commentUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/links> \"" + link + "\" .");
            if (!gif.equals(""))
                triplets.add(commentUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/gifFile> \"" + gif + "\" .");
            return String.join("\n", triplets);
		}
		else if (queryType.equals("LdbcUpdate8AddFriendship")) {
			long person1Id = Long.parseLong(extractWord(arguments, person1IdPattern));
			long person2Id = Long.parseLong(extractWord(arguments, person2IdPattern));
			DateFormat format1 = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.ENGLISH);
			Date creationDate = format1.parse(extractWord(arguments, creationDatePattern));
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


	private String companyUri(Long long1) {
		// TODO Auto-generated method stub
		return null;
	}

	private String universityUri(int int1) {
		// TODO Auto-generated method stub
		return null;
	}

	private String tagUri(String string) {
		// TODO Auto-generated method stub
		return null;
	}

	private String placeUri(long cityId) {
		// TODO Auto-generated method stub
		return null;
	}

	private String extractWord(String arguments, Pattern p) {
		Matcher m = p.matcher(arguments);
		m.find();
		return m.group(1);
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
