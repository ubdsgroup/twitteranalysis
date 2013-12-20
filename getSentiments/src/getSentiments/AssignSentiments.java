package getSentiments;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;


public class AssignSentiments 
{
	public static void main(String args[])
	{
		int verifySony = 0;
		int verifyMSFT = 0;
		
		// read sentiments file
		String currentLine = null;
		String tokens[]= null;
		String tweetWords[]= null;
		String lineItem_senti = null;
		String lineItem_mood =  null;
		String probs[] = null;
		double sadProb =0;
		double happyProb = 0;
		double senti_happy = 0;
		double senti_sad = 0;
		
		// SONY / MSFT / Both -- to whom does the tweet belong
		boolean Sony =false;
		boolean MSFT =false;
		boolean Both =false;
		
		String tweetHashTags[]; 
		
		HashMap<String,String> sentimentDictionary =new HashMap<String,String>();
		HashMap<String,Integer> sonyHashTags =  new HashMap<String,Integer>();
		HashMap<String,Integer> msftHashTags = new HashMap<String,Integer>();
		
		String key = null;
		String value=null;
		
		// to parse the tweet created date_time field
		String dateTokens[];
		String timeTokens[];
		String prevDate = null;
		int prevHour = 25; // cannot be 25 but for the first pass -- makes sense ?
		int hour;
		String date;
		int fieldCount = 19;
		int msftTweetCount_perHour = 0;
		int sonyTweetCount_perHour = 0;
		double msftProbHappy_perHour = 0;
		double sonyProbHappy_perHour = 0;
		
		// variables that keep track of row validity
		int totalTweets = 0;
		int noAttributionResultCount = 0;
		int msftCount = 0;
		int sonyCount = 0;
		int bothCount = 0;
		int invalidRowsCount = 0;

		
		// sony hashtags we looked for
		sonyHashTags.put("ps4",1);
		sonyHashTags.put("playstation4",2);
		sonyHashTags.put("playstation",3);
		
		// msft hashtags we looked for
		msftHashTags.put("xboxone",1);
		msftHashTags.put("onespace",2);
		
		// files to read from and write to
		BufferedWriter tweetNsentiment = null;
		BufferedWriter mood_ofThe_hourMSFT = null;
		BufferedWriter mood_ofThe_hourSony = null;
		BufferedWriter noAttribution = null;
		BufferedReader twitterDataHandle= null;
		BufferedReader dictionaryDataHandle=null;
		
		
		/** COLUMNS IN THE TWITTER DATA PULL FILE
			1.  SCREEN_NAME
			2.  NAME
			3.  PROFILE_LOCATION
			4.  JOINING_DATE
			5.  FRIENDS_COUNT
			6.  FOLLOWERS_COUNT
			7.  FAVOURITES_COUNT
			8.  TWEE_ID
			9.  TWEET
			10. LATITUTE
			11. LONGITUTE
			12. TWEET_CREATED_AT
			13. HASH_TAGS
			14. HASH_TAG_COUNT
			15. URLS
			16. URL_COUNT
			17. IS_RETWEET
			18. IS_RETWEETED
		**/
		
		// tweetID -- tweet -- sentiment -- SONY -- MSFT -- Both -- date time which hour -- is_retweet
		String headerSentiment = "tweetID"+"\t"+"tweet"+"\t"+"senti_happy"+"\t"+"senti_sad"+"\t"+"SONY"+"\t"+"MSFT"+"\t"+"Both"+"\t"+"Date"+"\t"+"Which_Hour"+"\t"+"isRetweet"+"\n";
		// Date,Hour,tweetCount,probHappy
		String headerMood = "date"+","+"hour"+","+"tweetCount"+","+"probHappy"+"\n";
		
		try
		{
			tweetNsentiment      = new BufferedWriter(new FileWriter("/home/raja/Dev/Workspace/Java/tweetAndSenti.txt"));
			mood_ofThe_hourMSFT  = new BufferedWriter(new FileWriter("/home/raja/Dev/Workspace/Java/mood_ofThe_hourMSFT.csv"));
			mood_ofThe_hourSony  = new BufferedWriter(new FileWriter("/home/raja/Dev/Workspace/Java/mood_ofThe_hourSony.csv"));
			noAttribution        = new BufferedWriter(new FileWriter("/home/raja/Dev/Workspace/Java/noAttribution.txt"));
			twitterDataHandle    = new BufferedReader(new FileReader("/home/raja/Dev/Workspace/Java/data_"));
			dictionaryDataHandle = new BufferedReader(new FileReader("/home/raja/Dev/Workspace/Java/twitter_sentiment_list.csv"));

			// get the header into the file where we are going to commit data
			tweetNsentiment.write(headerSentiment);
			mood_ofThe_hourMSFT.write(headerMood);
			mood_ofThe_hourSony.write(headerMood);
			tweetNsentiment.flush();
			mood_ofThe_hourMSFT.flush();
			mood_ofThe_hourSony.flush();
			
			// ignoring column headings
			currentLine = dictionaryDataHandle.readLine();
			
			// store all keys in lowercase
			while ((currentLine = dictionaryDataHandle.readLine()) != null)
			{
				// tokenize it
				tokens = currentLine.split(",");
				if(tokens.length == 3)
				{
					key = tokens[0];
					key = key.toLowerCase();
					value = tokens[1]+","+tokens[2];
				}
				sentimentDictionary.put(key,value);
				key = null;
				value = null;
			}
			
			/*
			 * "#XboxOne","#OneSpace","@Xbox","#PS4","#PlayStation4","#PlayStation"
			 * Hashtags used --- shall help figure out if the tweet is about PS or XBox
			 * or Both
			 */
			
			//ignoring column headings
			currentLine = twitterDataHandle.readLine();
			noAttribution.write(currentLine);
			noAttribution.flush();
			
			
			// need to also find out if the tweet is about MSFT / SONY
			while((currentLine = twitterDataHandle.readLine())!=null)
			{
				totalTweets++;
				tokens = currentLine.split("\t");
				/**
				 *  there are some rouge lines in the date which when tokenized using "\t" we get
				 *  more/less tokens than admissible tokens (which is fieldCount)
				 *  we shall not process these rouge line-items 
				 */  
				if(tokens.length < fieldCount || tokens.length > fieldCount)
				{
					invalidRowsCount++; // we discard such rows and also keep a count of such lines
					continue;
				}
				if(tokens.length > 0)
				{
					tweetWords = tokens[8].split(" ");
					for(int i =0;i<tweetWords.length;i++)
					{
						/*
						 * In our tweet search we also searched
						 * we are looking for tweets that have @Xbox
						 * but since it is not a hashtag we shan't be able to
						 * catch it when attributing tweets to products (MSFT / SONY)
						 * lower down in code
						 */
						
						
						if(tweetWords[i].toLowerCase().contains("@xbox") && MSFT == false)
						{
							msftCount++;
							MSFT = true;
						}
						// search using lowercase
						value = sentimentDictionary.get(tweetWords[i].toLowerCase());
						if(value != null)
						{
							probs = value.split(",");
							happyProb =happyProb+new Double(probs[0]);
							sadProb =  sadProb+new Double(probs[1]);
						}
					}
					
					// calculate sentiment
					senti_happy = 1/(Math.exp(sadProb - happyProb) +1); 
					senti_sad = 1-senti_happy;
										
					/**
					 * tweet attribution
					 * to whom should we attribute this tweet SONY / MSFT
					 */
					tweetHashTags = tokens[12].split(",");
					for(int i =0; i<tweetHashTags.length;i++)
					{
						if(!Sony) // only check if attribution not done already
						{
							if(Sony = sonyHashTags.containsKey(tweetHashTags[i].toLowerCase()))
							{
								sonyCount++;
							}
						}
						if(!MSFT)
						{
							 if(MSFT = msftHashTags.containsKey(tweetHashTags[i].toLowerCase()))
							 {
								msftCount++; 
							 }
						}
					}
					
					/**
					 * A case like this shall happen
					 * only when we are not able to attribute tweet 
					 * to either MSFT / SONY
					 * MSFT == FALSE / Sony == FALSE
					 * 
					 * this should not occur -- FIXME
					 * We should have checked each word in the tweet
					 * for a match with our search query strings
					 * instead of only looking for only for hashtag presence -- done above
					 * IF our search query string are not Hashtags we need to do work by word search -- FIXME
					 */
					if(!(Sony || MSFT))  
					{
						/**
						 * keep count of such tweets
						 * record the same in a purge list file
						 * and continue
						 */
						noAttributionResultCount++; 
						noAttribution.write(tokens[8]+"\n");
						// reverse the addition as this is an invalid record
						happyProb =happyProb-new Double(probs[0]);
						sadProb =  sadProb-new Double(probs[1]);
						continue;
					}
					
					// should this tweet be attributed to both SONY and MSFT
					Both = Sony && MSFT;
					
					if(Both) bothCount++;
					
					/**
					 * need to parse the <created at date> field and get hourly statistics 
					 * sample --- Wed Dec 18 00:23:55 EST 2013
					 */
					dateTokens = tokens[11].split(" ");
					timeTokens = dateTokens[3].split(":");
					hour = new Integer(timeTokens[0]);
					hour++;
					date = dateTokens[1]+dateTokens[2]+"_"+dateTokens[5];
					 
					/**
					 * Handle first pass
					 */
					if(prevHour == 25)
					{
						prevHour = hour;
						prevDate = date;
					}
					
					/*
					 * New Hour -- 
					 * assumption is that we are getting some tweets every hour -- otherwise
					 * we would have to  (prevHour != hour && prevDate.equals(date)) 
					 * in place of prevHour != hour 
					 * 
					 * Since we are tracking two products 
					 * we need to find the mood for both product 
					 * Sony and MSFT
					 * 
					 */
					if(prevHour != hour)
					{
						
						// commit prev hour info into hour_mood file
						msftProbHappy_perHour = msftProbHappy_perHour/msftTweetCount_perHour;
						sonyProbHappy_perHour = sonyProbHappy_perHour/sonyTweetCount_perHour;
						
						// check if the numbers match -- will be output to the console
						verifySony += sonyTweetCount_perHour;
						verifyMSFT += msftTweetCount_perHour;
						
						lineItem_mood = prevDate+","+prevHour+","+msftTweetCount_perHour+","+msftProbHappy_perHour+"\n";
						mood_ofThe_hourMSFT.write(lineItem_mood);
						mood_ofThe_hourMSFT.flush();
						
						lineItem_mood = prevDate+","+prevHour+","+sonyTweetCount_perHour+","+sonyProbHappy_perHour+"\n";
						mood_ofThe_hourSony.write(lineItem_mood);
						mood_ofThe_hourSony.flush();
						
						lineItem_mood = null;
						
						// time for reset of info
						prevHour = hour;
						prevDate = date;
					
						msftTweetCount_perHour = 0;
						msftProbHappy_perHour = 0.0;
						
						sonyTweetCount_perHour = 0;
						sonyProbHappy_perHour = 0.0;
						
						if(MSFT)
						{
							msftTweetCount_perHour++;
							msftProbHappy_perHour += senti_happy;
						}
						if(Sony)
						{
							sonyTweetCount_perHour++;
							sonyProbHappy_perHour += senti_happy;
						}
					}
					else // we are in the same hour
					{
						if(MSFT)
						{
							msftTweetCount_perHour++;
							msftProbHappy_perHour += senti_happy;
						}
						if(Sony)
						{
							sonyTweetCount_perHour++;
							sonyProbHappy_perHour += senti_happy;
						}
					}
					
					/*
					 * create the lineItem to be stored
					 * tweetID -- tweet -- senti_happy --- senti_sad -- SONY -- MSFT -- Both -- date -- which hour -- is_retweet
					 */
					lineItem_senti = tokens[7]+"\t"+tokens[8]+"\t"+senti_happy+"\t"+senti_sad+"\t"+Sony+"\t"+MSFT+"\t"+Both+"\t"+date+"\t"+hour+"\t"+tokens[11]+"\t"+tokens[16]+"\n";
					
					// need to store this value somewhere
					tweetNsentiment.write(lineItem_senti);
					tweetNsentiment.flush();
				
					
					// clear all recordings
					lineItem_senti = null;
					happyProb = 0;
					sadProb = 0;
					senti_happy = 0;
					senti_sad = 0;
					Sony = MSFT = Both = false;
				}
			}	
			
			/**
			 *    We still have some data to commit
			 *    In this case we are lucky since
			 *    the extra data that we have to commit
			 *    belongs to a new hour
			 * IF the data belonged to the same hour -- well
			 *    i think we should be fine -- but need to verify -- FIXME
			 **/
			msftProbHappy_perHour = msftProbHappy_perHour/msftTweetCount_perHour;
			sonyProbHappy_perHour = sonyProbHappy_perHour/sonyTweetCount_perHour;
			
			verifySony += sonyTweetCount_perHour;
			verifyMSFT += msftTweetCount_perHour;
			
			lineItem_mood = prevDate+","+prevHour+","+msftTweetCount_perHour+","+msftProbHappy_perHour+"\n";
			mood_ofThe_hourMSFT.write(lineItem_mood);
			mood_ofThe_hourMSFT.flush();
			
			lineItem_mood = prevDate+","+prevHour+","+sonyTweetCount_perHour+","+sonyProbHappy_perHour+"\n";
			mood_ofThe_hourSony.write(lineItem_mood);
			mood_ofThe_hourSony.flush();
			
			System.out.println("Verification -- simple statistics");
			System.out.println("************************************************");
			System.out.println("total tweets="+totalTweets);
			System.out.println("tweets Split:\n attributed to msft: "+msftCount+"\n attributed to sony: "+sonyCount+"\n attributed to both: "+bothCount+"\n with no attribution: "+noAttributionResultCount+"\n with incomplete rows: "+invalidRowsCount);
			System.out.println("************************************************");
			System.out.println("Verification count sony="+verifySony+" msft="+verifyMSFT);
			System.out.println("************************************************");
			System.out.println("sum total as per attribution : "+(msftCount+sonyCount-bothCount+noAttributionResultCount+invalidRowsCount));
			System.out.println("sum total by per hour count : "+(verifyMSFT+verifySony-bothCount+noAttributionResultCount+invalidRowsCount));
			System.out.println("************************************************");
			
		}
		catch(FileNotFoundException fneException)
		{
			fneException.printStackTrace();
		}
		catch(IOException ioException)
		{
			ioException.printStackTrace();
		}
		finally
		{
			try
			{
				if (tweetNsentiment!= null)tweetNsentiment.close();
				if (mood_ofThe_hourMSFT!= null)mood_ofThe_hourMSFT.close();
				if (mood_ofThe_hourSony!= null) mood_ofThe_hourSony.close();
				if (noAttribution!= null) noAttribution.close(); 
				if (twitterDataHandle!= null) twitterDataHandle.close();
				if (dictionaryDataHandle!= null) dictionaryDataHandle.close();
			} 
			catch (IOException ex) 
			{
				ex.printStackTrace();
			}
		}
	}
}
