import re
from dotenv import load_dotenv
import inflect
from urllib.parse import urlparse
from nltk.corpus import wordnet

def extract_unique_words_advanced(data_list):
    """
    Processes user query to generate a list of keywords to search the database for.
    """
    p = inflect.engine()
    unique_words_set = set()
    delimiters = r'[\x20-\x2F\x39-\x40\x5B-\x60\x7B-\x7E]+'

    for item in data_list:
        tokens = re.split(delimiters, str(item))
        for token in tokens:
            token_lower = token.lower()
            if token_lower.startswith('http'):
                try:
                    parsed_url = urlparse(token_lower)
                    netloc = parsed_url.netloc
                    domain_parts = netloc.split('.')
                    if len(domain_parts) > 1 and domain_parts[0] == 'www':
                        domain_name = domain_parts[1]
                    else:
                        domain_name = domain_parts[0]
                    if domain_name:
                        unique_words_set.add(domain_name)
                except Exception:
                    continue
            else:
                clean_word = token_lower.strip('.,!?:;') 
                if clean_word:
                    unique_words_set.add(clean_word)

        for word in list(unique_words_set) :
            plural = p.plural(word)
            singular = p.singular_noun(word)
            if plural :
                unique_words_set.add(plural)
            if singular:
                unique_words_set.add(singular)

        for word in list(unique_words_set) :
            for syn in wordnet.synsets(word) :
                for lemma in syn.lemmas():
                    synonym = lemma.name().replace('_', ' ')
                    unique_words_set.add(synonym) 


    return list(unique_words_set)
