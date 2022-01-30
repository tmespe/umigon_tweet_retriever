# Umigon tweet retriever
Gets tweet for a given term and returns the tweet if the given term is considered as positive in the context of the tweet

## Requirements
- pandas~=1.3.5
- requests~=2.27.1
- python-dotenv~=0.19.2
- twarc~=2.8.3
- openpyxl~=3.0.9

## Usage
Clone the repo and install requirements using pip install -r requirements.txt. Create a .env file with the following:

- CONSUMER_KEY=yourkey
- CONSUMER_SECRET=yourkey
- ACCESS_TOKEN=yourkey
- ACCESS_TOKEN_SECRET=yourkey

or if using twarcv2 with Twitters API v2

- BEARER=yourkey

corresponding to your Twitter API authentication. 

To run use python main.py -f filename_with_terms 
-o filename_to_output to [-s sheet_name] like so:

python main.py -f en.xlsx -o terms.csv -s 1_positive tone


The script will return a csv file with the term, tweet_id and tweet_text for each term in the input file if the sentiment is positive in the given context of a tweet. If there are no tweets where the term is evaluated as positive the term is output in failed_terms.csv.
