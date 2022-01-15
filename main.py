import argparse
import pandas as pd
import os
from datetime import datetime, timedelta
from time import sleep
import pathlib

from typing import Union, Any
from itertools import islice

import requests
from dotenv import load_dotenv
from twarc import Twarc

load_dotenv()

# Initialize twitter authentication
BEARER = os.getenv("BEARER_TOKEN")
CONSUMER_KEY = os.getenv("CONSUMER_KEY")
CONSUMER_SECRET = os.getenv("CONSUMER_SECRET")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
ACCESS_TOKEN_SECRET = os.getenv("ACCESS_TOKEN_SECRET")

# Set URL for webservice used to evaluate term sentiment
UMIGON_WEBSERVICE = "https://test.nocodefunctions.com/api/sentimentForOneTermInAText/"

# Initialize argparse for parsing CLI arguments
my_args = argparse.ArgumentParser()

my_args.add_argument(
    "-t", "--term", metavar="term", type=str, help="Term to evaluate", required=False
)

my_args.add_argument(
    "-f", "--file", metavar="file", type=str, help="File to read", required=False
)

# my_args.add_argument(
#     "-c", "--context", metavar="context", narges="+", type=str, help="File to read", required=False
# )

my_args.add_argument(
    "-o", "--output", metavar="out", type=str, help="Output file name", required=True
)

args = my_args.parse_args()
if not args.output.endswith(".csv"):
    print("Output file needs to be a csv file")
    exit()


# Read file
def read_file(file):
    file_extension = pathlib.Path(file).suffix
    if file_extension == ".csv" or file_extension == ".txt":
        file = read_file(args.file)
        with open(file, 'r') as f:
            terms = [line.split("\t")[0] for line in file]
            return terms
    elif file_extension == ".xlsx":
        file = pd.read_excel(args.file, sheet_name="1_positive tone")
        return file.iloc[:, 0].values
    else:
        print("Invalid input file type. Must be .txt, .csv or .xlsx")


class Term:

    def __init__(self, term, language="en", n_tweets=1) -> None:
        self.term = term
        self.language = language
        self.tweets = self.twitter_search(n_tweets=n_tweets)

    def __repr__(self) -> str:
        """
        Prints the sentiment of the erm
        :return: String representing sentiment
        """
        return f"{self.term}"

    def umigon_search(self, context) -> str:
        """
        Query's the Umigon webservice for a given term and returns the sentiment for a given context
        :param context: Context of the term e.g a tweet
        :param language: Language of the term and context
        :return: string
        """
        r = requests.get(f"{UMIGON_WEBSERVICE}{self.language}", params={"term": self.term, "text": context})
        if r.status_code == 200:
            sentiment = r.text
            return sentiment
        else:
            return "No sentiment"

    def twitter_search(self, n_tweets) -> None:
        """
        Searches twitter for a given term and saves the amount of tweets specified in n_tweets to a csv file.
        By default appends ("a"), but can be overriden to write ("w) with the "mode" parameter
        :n_tweets: int Number of tweets to save
        :mode: str Mode for writing to file a/w (append,write)
        :return: None
        """
        client = Twarc(consumer_key=CONSUMER_KEY, consumer_secret=CONSUMER_SECRET, access_token=ACCESS_TOKEN,
                       access_token_secret=ACCESS_TOKEN_SECRET)

        end_time = datetime.utcnow()
        start_time = end_time - timedelta(days=6)
        df = pd.DataFrame(columns=["term", "id", "full_text"])

        tweets = client.search(self.term, lang=self.language)
        for tweet in tweets:
            tweet_text = tweet["full_text"]
            if n_tweets > 0:
                # if not "RT @" in tweet["full_text"] and len(tweet["full_text"]) >= 20:
                if len(tweet_text) >= 20 and tweet_text not in df["full_text"].values:
                    if self.term in tweet_text:
                        sentiment = self.umigon_search(context=tweet_text)
                        tweet["term"] = self.term
                        if sentiment == "positive":
                            df = df.append(tweet, ignore_index=True)
                            n_tweets -= 1
                else:
                    continue
            else:
                break
            # sleep(1)
            # self.twitter_search(n_tweets=n_tweets)

        return df[["term", "id", "full_text"]]

        # Twarc2 code using bearer token
        # query = f"{self.term} lang:{self.language}"
        #
        # search_results = client.search_all(query=query, start_time=start_time, end_time=end_time, max_results=100)
        #
        # # Twarc returns all Tweets for the criteria set above, so we page through the results
        # for page in search_results:
        #     # The Twitter API v2 returns the Tweet information and the user, media etc.  separately
        #     # so we use expansions.flatten to get all the information in a single JSON
        #     result = expansions.flatten(page)
        #     for tweet in result:
        #         # Here we are printing the full Tweet object JSON to the console
        #         print(json.dumps(tweet))

    def to_csv(self, mode="a") -> None:
        """
        Saves tweets for a given term to a csv file
        :param mode: a for append / w for overwrite
        :return: None
        """
        self.tweets.to_csv(args.output, mode=mode, header=False, index=False, encoding="UTF-8")


def main():
    if args.file:
        terms_list = read_file(args.file)
        for term in terms_list:
            Term(term).to_csv()
            print(f"Finished writing {term} to file")
            # term.find_sentiment()
            sleep(2)
    elif args.term:
        print(Term(args.term))


if __name__ == '__main__':
    main()

#
# lines = read_file("terms.txt")
# for line in lines[:5]:
#     print(line.split("\t")[0])
#
# terms = [Term(line.split("\t")[0], n_tweets=5) for line in lines[:5]]
# # x = terms[0].umigon_search("This thing is")
# for term in terms:
#     sleep(1)
#     # term.find_sentiment()
#     term.to_csv()
#
# # term = Term("sdfsdf")
# # term.umigon_search(context="")
