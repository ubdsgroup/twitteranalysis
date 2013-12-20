package twitterData_project;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;

import twitter4j.FilterQuery;
import twitter4j.GeoLocation;
import twitter4j.HashtagEntity;
import twitter4j.Place;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.URLEntity;
import twitter4j.User;
import twitter4j.conf.ConfigurationBuilder;

import java.util.ArrayList;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import javax.management.Query;


/*
 * 
Consumer key: HqP2dsSTrRsN1elmKiHg8g
Consumer secret : h5qekNSL8ihIzFgmXWT3UjaNX59B8PM7WVjB8Pa0 // not to be shared
Access Token : 2188807326-kgDtD8lUcz7YS6cxPuRicWtDkzcAqWSAjlY6aYa
Access Token secret : r9GrbJuKvsrW8Les3MmQkSnwID0MG19suCiODQ1r69C63
Request URL : https://api.twitter.com/1/
*/

public class TwitterDataPull
{
	public static void main(String[] args) 
    {
    	// create file for writing
        String fileName_prefix = "../twitterDataPull_";
        String fileName = null;
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	   	Date date = new Date();
		
		fileName  = fileName_prefix + dateFormat.format(date);
    	File storageFile = new File(fileName);
    	final BufferedWriter bw;
    	
    	String header = "SCREEN_NAME\tNAME\tPROFILE_LOCATION\tJOINING_DATE\tFRIENDS_COUNT\tFOLLOWERS_COUNT\tFAVOURITES_COUNT\tTWEE_ID\tTWEET\tLATITUTE\tLONGITUTE\t";
    	header += "TWEET_CREATED_AT\tHASH_TAGS\tHASH_TAG_COUNT\tURLS\tURL_COUNT\tIS_RETWEET\tIS_RETWEETED\tPLACE\n";
		try
        {
        	storageFile.createNewFile();
        	bw = new BufferedWriter(new FileWriter(storageFile));
        	bw.write(header);
        }
        catch(FileNotFoundException fnfex)
        {
        	fnfex.printStackTrace();
        	System.out.println("The storage file could not be found");
        	return;
        }
        catch(IOException IOex)
        {
        	IOex.printStackTrace();
        	System.out.println("Something is wrong with the create file thing");
        	return;

        }
        
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true);
        cb.setOAuthConsumerKey("HqP2dsSTrRsN1elmKiHg8g");
        cb.setOAuthConsumerSecret("h5qekNSL8ihIzFgmXWT3UjaNX59B8PM7WVjB8Pa0");
        cb.setOAuthAccessToken("2188807326-kgDtD8lUcz7YS6cxPuRicWtDkzcAqWSAjlY6aYa");
        cb.setOAuthAccessTokenSecret("r9GrbJuKvsrW8Les3MmQkSnwID0MG19suCiODQ1r69C63");

        cb.setIncludeEntitiesEnabled(true);
        cb.setJSONStoreEnabled(true);
        TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
        
        
        /*String test = "rajaram\n Rabindranath";
        System.out.println(test);
        
        String a  = test.replaceAll("\n",",");
        
        System.out.println(a);
        if(true) return;
        */
        
        
        // inner class -- listener
        StatusListener listener = new StatusListener()
        {
        	String lineItem = new String();
            String tagFields =  new String();
            String urlFields =  new String();
            String mt = "-";
            long tweetID;
            String profileLoc = "";
            String tweet = "";
            User user = null; 
            
            @Override
            public void onException(Exception arg0) 
            {
                System.out.println("There seems to be an execption");
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice arg0) 
            {
            	System.out.println("There seems to a deletion notice");
            }

            @Override
            public void onScrubGeo(long arg0, long arg1) 
            {
            	System.out.println("Some on scrub geo has been called");
            }


            @Override
            public void onTrackLimitationNotice(int arg0) 
            {
            	System.out.println("On track limitation");
                // TODO Auto-generated method stub

            }

			@Override
			public void onStallWarning(StallWarning arg0) 
			{
				System.out.println("On stall warning");
			}
            
            @Override
            public void onStatus(Status status)
            {
            	/**
            	 * Construct a line item as such: <tab separated entities>
            	 */
            	
                user = status.getUser();
                lineItem = "";
                
                /**
                 * <<<<USER INFORMATION>>>>
				 * User Screen Name
				 * User name -- profile i believe
				 * User location -- profile location
				 * User Language -- not taking this
				 * User Created at --
				 * User Friend Count
				 * User Follower Count
				 * User Favourite Count
				 */             
                
                
                lineItem += user.getScreenName()+"\t"+user.getName()+"\t"; 
                
                if(user.getLocation().equals("")) profileLoc =mt;
                else
                	profileLoc = user.getLocation();
                
                lineItem += profileLoc+"\t"+user.getCreatedAt()+"\t"+user.getFriendsCount()+"\t"+user.getFollowersCount()+"\t"+user.getFavouritesCount()+"\t";
                
               /**
                * <<<<TWEET INFORMATION>>>>
				 * TweetID
				 * Tweet
				 * Tweet Lang -- not taking this
				 * TweetGeoLocation -- Lat & Long of the place from where it was tweeted
				 * TweetCreated at 
				 * HashTags -- #tags associated with the tweet -- in a csv format
				 * Count #Tags -- count of hash tags in each tweet
				 * URLs --urls in the tweet <url_entity>:<url_entity>; and each entity has <url>,<expanded_url>
				 * # URLs -- number of URLs in the tweet
				 * isRetweet
				 * isRetweeted
				 * Place
				*/
                lineItem += status.getId()+"\t";
                
                tweet = status.getText().replaceAll("\n", ",");
                
                lineItem +=tweet+"\t";
                
                GeoLocation whereFrom = status.getGeoLocation();
                
                if(whereFrom != null)
                {
	                lineItem += whereFrom.getLatitude()+"\t"+whereFrom.getLongitude()+"\t";
	            }
                else
                {
                	lineItem +=mt+"\t"+mt+"\t";
                }
                
                
                URLEntity [] urls= status.getURLEntities(); 
                HashtagEntity[] hashtags =  status.getHashtagEntities();
                if (hashtags.length == 0)tagFields = mt;
                for(int i =0;i<hashtags.length;i++)
                {
                	tagFields += (i == hashtags.length-1) ? hashtags[i].getText():hashtags[i].getText()+",";
                }
                   
                
                if(urls.length == 0)urlFields = mt;
                String field;
                
                for(int i=0;i<urls.length;i++)
                {
                	field = urls[i].getURL()+","+urls[i].getExpandedURL();
                	urlFields += (i == urls.length-1) ? field:field+":";
                }

                lineItem += status.getCreatedAt()+"\t"+tagFields+"\t"+hashtags.length+"\t"+urlFields+"\t"+urls.length+"\t";
                lineItem += status.isRetweet()+"\t"+status.isRetweeted()+"\t";       
                
                urlFields = "";
                tagFields = "";
                
                Place pl = status.getPlace();
                
                if(pl != null)
                {
                	lineItem += pl.getName(); 
                }
                else
                {
                	lineItem += mt;
                }
                
                System.out.println("tweets@ "+status.getText());
                
                try
                {
                	bw.write(lineItem+"\n");
                	bw.flush();
                }
                catch(IOException ex)
                {
                	ex.printStackTrace();
                	System.out.println("Some problem in writing to file");
                }
                
            }


        };
        FilterQuery fq = new FilterQuery();
        
        String keywords[] = {"#XboxOne","#OneSpace","@Xbox","#PS4","#PlayStation4","#PlayStation"};
        String[] lang = {"en"}; // filter only english language tweets
        fq.language(lang);
        fq.track(keywords);
        
        twitterStream.addListener(listener);
        twitterStream.filter(fq);
    }
}
