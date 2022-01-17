import argparse
import pandas as pd
import os
from time import sleep
import pathlib

from typing import Union, Any

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
if pathlib.Path(args.file).suffix == ".xlsx":
    if not args.sheet_name:
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
        r = requests.get(f"{UMIGON_WEBSERVICE}{self.language}", params={"term": self.term, "text": context})
        # If language, term and context are valid webservice will return 200
        if r.status_code == 200:
            sentiment = r.text
            return sentiment
        else:
            return None

    def twitter_search(self, n_tweets: int) -> None:
        """
        Searches twitter for a given term and saves the amount of tweets specified in n_tweets to a csv file.
        By default appends ("a"), but can be overriden to write ("w) with the "mode" parameter
        :n_tweets: int Number of tweets to save
        :mode: str Mode for writing to file a/w (append,write)
        :return: None
        """
        # Connect to twitter API using Twarc
        client = Twarc(consumer_key=CONSUMER_KEY, consumer_secret=CONSUMER_SECRET, access_token=ACCESS_TOKEN,
                       access_token_secret=ACCESS_TOKEN_SECRET)

        # end_time = datetime.utcnow()
        # start_time = end_time - timedelta(days=6)

        # Initialise dataframe storing term, tweet_id and tweet text
        df = pd.DataFrame(columns=["term", "id", "full_text"])
        tweets = client.search(self.term, lang=self.language)

        # Loop over tweets and check if tweet text is longer than 20 chars and term is positive in context
        # else find another tweet
        for tweet in tweets:
            tweet_text = tweet["full_text"]
            if n_tweets > 0:
                # if not "RT @" in tweet["full_text"] and len(tweet["full_text"]) >= 20:
                if len(tweet_text) >= 20 and tweet_text not in df["full_text"].values:
                    # Check if term is positive in the tweet using the Umigon webservce
                    sentiment = self.umigon_search(context=tweet_text)
                    tweet["term"] = self.term
                    if sentiment == "positive":
                        df = df.append(tweet, ignore_index=True)
                        n_tweets -= 1
                else:
                    continue
            else:
                break

        return df[["term", "id", "full_text"]]

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
    if file_extension == ".csv" or file_extension == ".txt":
        if file_extension == ".txt":
            file = pd.read_csv(file, sep="\t")
        else:
            file = pd.read_csv(file)
    elif file_extension == ".xlsx":
        file = pd.read_excel(args.file, sheet_name=" ".join(args.sheet_name))
    else:
        print("Invalid input file type. Must be .txt, .csv or .xlsx")
    file = file.iloc[:, 0].dropna()
    return file


def main() -> None:
    """
    Reads a file containing a list of terms and writes tweets containing those terms to a csv file.
    :return: None
    """
    # Check if file argument provided with CLI and read file content if True
    if args.file:
        terms_list = read_file(args.file)
        # Loop over terms in file and save term, tweet_id and tweet text to csv file
        for term in terms_list:
            Term(term).to_csv()
            print(f"Finished writing {term} to file")
            sleep(2)
    elif args.term:
        print(Term(args.term))


if __name__ == '__main__':
    main()
