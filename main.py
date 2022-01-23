import argparse
from datetime import datetime, timedelta
import pandas as pd
import os
from time import sleep
import pathlib

from typing import Union, Any

import requests
from requests import ConnectionError, HTTPError
from dotenv import load_dotenv
from twarc import Twarc2, ensure_flattened

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

my_args.add_argument(
    "-s", "--sheet-name", metavar="sheet", type=str, nargs="*", help="Sheet name to read from xlsx", required=False
)

my_args.add_argument(
    "-o", "--output", metavar="out", type=str, help="Output file name", required=True
)

# Parse args and print error messages if requirements not followed
args = my_args.parse_args()
if not args.output.endswith(".csv"):
    print("Output file needs to be a csv file")
    exit()
if pathlib.Path(args.file).suffix == ".xlsx" and not args.sheet_name:
    print("Sheet name is required for xlsx file")
    exit()


class Term:

    def __init__(self, term: str, language: str = "en", n_tweets: int = 1) -> None:
        self.term = term
        self.language = language
        self.tweets = self.twitter_search(n_tweets=n_tweets)

    def __repr__(self) -> str:
        """
        Prints the sentiment of the erm
        :return: String representing sentiment
        """
        return f"{self.term}"

    def umigon_search(self, context: str) -> Union[str, None]:
        """
        Query's the Umigon webservice for a given term and returns the sentiment for a given context
        :param context: Context of the term e.g a tweet
        :param language: Language of the term and context
        :return: string
        """
        try:
            r = requests.get(f"{UMIGON_WEBSERVICE}{self.language}", params={"term": self.term, "text": context})
            # If language, term and context are valid webservice will return 200
            return r.text if r.status_code == 200 else None
        except (ConnectionError, HTTPError):
            print("Connection error")
            sleep(60)
            self.umigon_search(context=context)

    def twitter_search(self, n_tweets: int) -> None:
        """
        Searches twitter for a given term and saves the amount of tweets specified in n_tweets to a csv file.
        By default appends ("a"), but can be overriden to write ("w) with the "mode" parameter
        :n_tweets: int Number of tweets to save
        :mode: str Mode for writing to file a/w (append,write)
        :return: None
        """
        # Connect to twitter API using Twarc
        #client = Twarc(consumer_key=CONSUMER_KEY, consumer_secret=CONSUMER_SECRET, access_token=ACCESS_TOKEN,
        #               access_token_secret=ACCESS_TOKEN_SECRET)

        client = Twarc2(bearer_token=BEARER)

        end_time = datetime.utcnow() - timedelta(days=2)
        start_time = end_time - timedelta(days=6)

        # Initialise dataframe storing term, tweet_id and tweet text
        df = pd.DataFrame(columns=["term", "id", "text"])
        #tweets = client.search(self.term, lang=self.language)
        search_results = client.search_recent(query=f'"{self.term} :)" lang:{self.language}')

        # Loop over tweets and check if tweet text is longer than 20 chars and term is positive in context
        # else find another tweet
        try_count = 0
        for page in search_results:
            for tweet in ensure_flattened(page): 
                if n_tweets > 0 and try_count < 20:
                    tweet_text = tweet["text"]
                    if len(tweet_text) < 20 or tweet_text in df["text"].values:
                        continue
                    # Check if term is positive in the tweet using the Umigon webservce
                    sentiment = self.umigon_search(context=tweet_text)
                    tweet["term"] = self.term
                    if sentiment == "positive":
                        df = df.append(tweet, ignore_index=True)
                        n_tweets -= 1
                    else:
                        try_count += 1
                elif try_count >= 20:
                    return None

                else:
                    break
            break

        return df[["term", "id", "text"]]

    def to_csv(self, mode: str = "a") -> None:
        """
        Saves tweets for a given term to a csv file
        :param mode: a for append / w for overwrite
        :return: None
        """
        self.tweets.to_csv(args.output, mode=mode, header=False, index=False, encoding="UTF-8")


# Read and open a csv, text or xlsx file
def read_file(file: str) -> pd.DataFrame:
    """
    Reads a .txt, .csv or .xslx file with a list of terms and returns a dataframe containing all the terms.
    Terms must be in the first column. For .txt files "\t" separator is assumed. For csv files pandas will try
    to autodetect separaotr
    :param file: Filename to read
    :return: pd.Pataframe Dataframe containing terms
    """
    file_extension = pathlib.Path(file).suffix
    if file_extension == ".csv":
        file = pd.read_csv(file)
    elif file_extension == ".txt":
        file = pd.read_csv(file, sep="\t")
    elif file_extension == ".xlsx":
        file = pd.read_excel(args.file, sheet_name=" ".join(args.sheet_name))
    else:
        print("Invalid input file type. Must be .txt, .csv or .xlsx")
    file = file.iloc[:, 0].dropna()
    return file

def parse_terms(terms_list):
    timeouts = []
    for term in terms_list:
        parsed_term = Term(term)
        if parsed_term.tweets is not None:
            #print(f"Writing {term} to csv")
            parsed_term.to_csv()
            print(f"Finished writing {term} to file")
        else:
            timeouts.append(term)
            print(f"{term} timed out. Will retry later")
    return timeouts



def main() -> None:
    """
    Reads a file containing a list of terms and writes tweets containing those terms to a csv file.
    :return: None
    """
    # Check if file argument provided with CLI and read file content if True
    if args.file:
        terms_list = read_file(args.file)
        # Loop over terms in file and save term, tweet_id and tweet text to csv file
        timeouts = parse_terms(terms_list)
            #sleep(2)
        while len(timeouts) > 0:
            parse_terms(timeouts)

    elif args.term:
        print(Term(args.term))


if __name__ == '__main__':
    main()

