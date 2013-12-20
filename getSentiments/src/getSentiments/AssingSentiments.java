package getSentiments;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;


public class AssingSentiments 
{
	public static void main(String args[])
	{
		// read sentiments file
		String currentLine = null;
		String tokens[]= null;
		String tweetWords[]= null;
		String lineItem = null;
		double sadProb =0;
		double happyProb = 0;
		double senti_happy = 0;
		double senti_sad = 0;
		
		// SONY / MSFT / Both -- to whom does the tweet belong
		boolean Sony =false;
		boolean MSFT =false;
		boolean Both =false;
		
		String hashTags[]; 
		
		HashMap<String,String> sentimentDictionary =new HashMap<String,String>();
		HashMap<String,Integer> sonyHashTags =  new HashMap<String,Integer>();
		HashMap<String,Integer> msftHashTags = new HashMap<String,Integer>();
		
		String key = null;
		String value=null;
		
		// to parse the tweet created date_time field
		String dateTokens[];
		String timeTokens[];
		int hour;
		String date;
		
		
		int noResult = 0;
		int msftCount = 0;
		int sonyCount = 0;
		int problemo = 0;
		int wicked = 0;
		
		// sony hashtags we looked for
		sonyHashTags.put("ps4",1);
		sonyHashTags.put("playstation4",2);
		sonyHashTags.put("playstation",3);
		
		// msft hashtags we looked for
		msftHashTags.put("xboxone",1);
		msftHashTags.put("onespace",2);
		
		
		// tweetID -- tweet -- sentiment -- SONY -- MSFT -- Both -- date time which hour -- is_retweet
		String header = "tweetID"+"\t"+"tweet"+"\t"+"senti_happy"+"\t"+"senti_sad"+"\t"+"SONY"+"\t"+"MSFT"+"\t"+"Both"+"\t"+"Date"+"\t"+"Which_Hour"+"\t"+"isRetweet"+"\n";

		// get the dictionary ready
		try
		{
			BufferedWriter tweetNsentiment = new BufferedWriter(new FileWriter("/home/raja/Dev/Workspace/Java/tweetAndSenti.txt"));
			BufferedWriter purge =  new BufferedWriter(new FileWriter("/home/raja/Dev/Workspace/Java/purge.txt"));
			BufferedReader twitterDataHandle = new BufferedReader(new FileReader("/home/raja/Dev/Workspace/Java/data"));
			BufferedReader dictionaryDataHandle = new BufferedReader(new FileReader("/home/raja/Dev/Workspace/Java/twitter_sentiment_list.csv"));
			
			// get the header into the file where we are going to commit data
			tweetNsentiment.write(header);
			
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
			
			String probs[] = null;
			
			//ignoring column headings
			currentLine = twitterDataHandle.readLine();
			
			// need to also find out if the tweet is about MSFT / SONY
			while((currentLine = twitterDataHandle.readLine())!=null)
			{
				tokens = currentLine.split("\t");
				if(tokens.length < 19 || tokens.length > 19)
				{
					problemo++;
					continue;
				}
				if(tokens.length > 0)
				{
					//System.out.println("The tweet is ="+tokens[8]);
					tweetWords = tokens[8].split(" ");
					for(int i =0;i<tweetWords.length;i++)
					{
						//if(tweetWords[i].equalsIgnoreCase("@Xbox"))
						if(tweetWords[i].toLowerCase().contains("@xbox"))
						{
							MSFT = MSFT || true;
							msftCount++;
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
										
					// to whom should we attribute this tweet SONY / MSFT
					//System.out.println(currentLine);
					hashTags = tokens[12].split(",");
					for(int i =0; i<hashTags.length;i++)
					{
						if(!Sony)
						{
							Sony = sonyHashTags.containsKey(hashTags[i].toLowerCase());
							if(Sony) sonyCount++;
						}
						if(!MSFT)
						{
							MSFT = msftHashTags.containsKey(hashTags[i].toLowerCase());
							if(MSFT) msftCount++; 
						}
					}
					
					/*
					 * this should not happen -- but if it does 
					 * we need to handle this case
					 */
					if(!(Sony || MSFT))
					{
						noResult++;
						purge.write(tokens[8]+"\n");
					}
					
					// should this tweet be attributed to both SONY and MSFT
					Both = Sony && MSFT;
					
					// need to parse the created at date -- field and then find aggregate based on that
					// Thu Nov 21 17:34:07 EST 2013
					// date parsing operations
					
					if(tokens.length > 19)
					{
						wicked++;
						continue;
					}
					dateTokens = tokens[11].split(" ");
					System.out.println("the tokens ="+dateTokens.length);
					System.out.println("The tweet"+tokens[8]+" tot tokens "+tokens.length+"tweetID "+tokens[7]);
					timeTokens = dateTokens[3].split(":");
					hour = new Integer(timeTokens[0]);
					hour++;
					date = dateTokens[1]+dateTokens[2];
					
					// create the lineItem to be stored
					// tweetID -- tweet -- senti_happy --- senti_sad -- SONY -- MSFT -- Both -- date -- which hour -- is_retweet
					lineItem = tokens[7]+"\t"+tokens[8]+"\t"+senti_happy+"\t"+senti_sad+"\t"+Sony+"\t"+MSFT+"\t"+Both+"\t"+date+"\t"+hour+"\t"+tokens[11]+"\t"+tokens[16]+"\n";
					
					// need to store this value somewhere
					tweetNsentiment.write(lineItem);
					tweetNsentiment.flush();
				
					// clear all recordings
					lineItem = null;
					happyProb = 0;
					sadProb = 0;
					senti_happy = 0;
					senti_sad = 0;
					Sony = MSFT = Both = false;
				}
			}	
			System.out.println(" msft "+msftCount+" sony "+sonyCount+" no result "+noResult+" problemo"+problemo+" wicked"+wicked);
			
		}
		catch(FileNotFoundException fneException)
		{
			fneException.printStackTrace();
		}
		catch(IOException ioException)
		{
			ioException.printStackTrace();
		}
	}
}
