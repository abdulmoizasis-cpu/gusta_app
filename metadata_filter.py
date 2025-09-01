import psycopg2
import re
from dotenv import load_dotenv
import os
import inflect
from urllib.parse import urlparse
import nltk
from nltk.corpus import wordnet
from nltk.stem import WordNetLemmatizer

# --- Keyword Extraction Logic (from extract_keywords.py) ---
# This section contains the functions to process text into a list of unique keywords.
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


def test_keyword_filter(user_queries: list[str]):
    """
    Connects to the database and tests metadata filtering for a list of user queries.
    """
    conn = None
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT
        )
        print("Successfully connected to the database.")

        # Loop through each user query to be tested
        for query in user_queries:
            print(f"\n--- Testing Query: '{query}' ---")
            
            # 1. Split the user query into a list of unique keywords
            user_keywords = extract_unique_words_advanced([query])
            if not user_keywords:
                print("Could not extract any keywords from the query.")
                continue
            
            print(f"  - Extracted user keywords: {user_keywords}")
            
            # 2. Execute the SQL query to find matches and count keyword overlap
            with conn.cursor() as cursor:
                # This query finds all columns where the 'keywords' array has any overlap (&&)
                # with the user's keywords list.
                # It also calculates how many words from the user's list are in each column's list.
                cursor.execute("""
                    SELECT
                        column_name,
                        -- This part calculates the count of intersecting keywords
                        (SELECT COUNT(*)
                         FROM unnest(keywords) as k
                         WHERE k = ANY(%s)) as match_count,
                        keywords
                    FROM
                        public.column_embeddings
                    WHERE
                        keywords && %s
                    ORDER BY
                        match_count DESC;
                """, (user_keywords, user_keywords))
                
                results = cursor.fetchall()

            # 3. Display the results
            if results:
                print("  - Found matching columns (ranked by keyword count):")
                for column_name, match_count, db_keywords in results:
                    matched_keywords = list(set(user_keywords) & set(db_keywords))
                    print(f"    -> {column_name} (Matches: {match_count}) - Matched: {matched_keywords}")
            else:
                print("  - No columns found containing any of the keywords.")

    except psycopg2.Error as e:
        print(f"Database error: {e}")
    finally:
        if conn:
            conn.close()
            print("\nDatabase connection closed.")

# --- Run the Test ---
if __name__ == "__main__":
    # Define a list of user queries you want to test
    queries_to_test = [
        "Which facilities of Merck are GMP compliance?"
    ]
    
    test_keyword_filter(queries_to_test)



