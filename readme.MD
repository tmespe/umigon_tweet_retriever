# Umigon tweet retriever
Gets tweet for a given term and returns the tweet if the given term is considered as positive in the context of the tweet

## Requirements
- pandas~=1.3.5
- requests~=2.27.1
- python-dotenv~=0.19.2
- twarc~=2.8.3
- openpyxl~=3.0.9

## Usage
Clone the repo and install requirements using pip install -r requirements.txt. To run use python main.py -f filename_with_terms 
-o filename_to_output to [-s sheet_name] like so:

python main.py -f en.xlsx -o terms.csv -s 1_positive tone


The script will return a csv file with the term, tweet_id and tweet_text for each term in the input file.
