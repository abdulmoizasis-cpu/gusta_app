import re
from dotenv import load_dotenv
from urllib.parse import urlparse

def is_numeric_string(s):
    """Checks if a string can be converted to a float."""
    if not isinstance(s, str):
        return False
    try:
        float(s)
        return True
    except (ValueError, TypeError):
        return False
    
def is_text_data(data_list, threshold=0.5):
    """
    Checks if a list contains a majority of true non-numeric text data,
    even if numbers are stored as strings.
    """
    if not data_list:
        return False

    text_item_count = 0
    valid_items = 0
    
    for item in data_list:
        if item is not None and str(item).strip() != '':
            valid_items += 1
            if isinstance(item, str) and not is_numeric_string(item):
                text_item_count += 1
    
    if valid_items == 0:
        return False

    return (text_item_count / valid_items) > threshold

def extract_unique_words_advanced(sample_data):
    """
    Processes sample_data or the column name to extract unique keywords.
    """
    unique_words_set = set()
    delimiters = r'[\x20-\x2F\x39-\x40\x5B-\x60\x7B-\x7E]+'

    for item in sample_data:
        if isinstance(item, str) and not is_numeric_string(item): #only performs the keywords extraction if the sample item is non numeric.
            cleaned_string = re.sub(delimiters, ' ', item)
            tokens = cleaned_string.strip().split()

            for token in tokens:
                token_lower = token.lower()
                if token_lower.startswith('http'): #edge case for URLs
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

    return list(unique_words_set)

def main() :
    sample_data = ['Rare/Orphan Diseases', 'Bladder Cancer\nBlood/Coagulation Disorders\nBreast Cancer\nCancer\nCardiovascular Disorders\nCNS Disorders\nDermatology/Skin Diseases\nDiabetes\nEndocrine/Metabolic Disorders\nGastric Cancer\nGastrointestinal Diseases\nGenetic Disorders\nGenitourinary Diseases\nHematologic Diseases\nHematological Cancers\nInfectious Diseases\nInflammation/Immune\nKidney Diseases\nLeukemia\nLiver Diseases\nLung Cancer\nLymphoma\nMetastatic Cancers\nMultiple Myeloma\nMusculoskeletal Diseases\nNAFLD/NASH\nNeurodegenerative Diseases\nObesity\nOphthalmic Diseases\nOvarian Cancer\nPain Management\nPancreatic Cancer\nProstate Cancer\nRare/Orphan Diseases\nSarcoma Cancers\nSolid Cancers\nWomen`s/Prenatal Health', 'Acute Kidney Injury\nAllergy\nALS\nAnemia\nAnesthesia/Pain Control\nAnxiety\nAsthma\nBladder Cancer\nBlood/Coagulation Disorders\nBrain Cancer\nBreast Cancer\nCancer\nCardiovascular Disorders\nCNS Disorders\nColorectal Cancer\nDepression\nDermatology/Skin Diseases\nDiabetes\nDry Eye Syndrome\nEndocrine/Metabolic Disorders\nFibrotic Diseases\nGastric Cancer\nGastrointestinal Diseases\nGenitourinary Diseases\nHematological Cancers\nImmuno-Oncology\nImmunotherapy\nInflammation/Immune\nKidney Diseases\nLeukemia\nLiver Cancer\nLiver Diseases\nLiver Fibrosis\nLung Cancer\nLymphoma\nMelanoma\nMen`s Health\nMultiple Sclerosis\nNAFLD/NASH\nNeurodegenerative Diseases\nNewborn Health\nObesity\nOphthalmic Diseases\nOsteoarthritis\nOvarian Cancer\nPancreatic Cancer\nParkinson`s Disease\nPregnancy/Fertility\nPrenatal Heath\nPsychiatric Disorders\nRare/Orphan Diseases\nRenal/Kidney Cancer\nRespiratory Diseases\nRetinal Diseases\nRheumatoid Arthritis\nSchizophrenia\nSolid Cancers\nWomen`s/Prenatal Health', 'Anesthesia/Pain Control\nBlood/Coagulation Disorders\nCancer\nCardiovascular Disorders\nCNS Disorders\nEndocrine/Metabolic Disorders\nGastrointestinal Diseases\nGenitourinary Diseases\nHematologic Diseases\nInfectious Diseases\nInflammation/Immune\nMusculoskeletal Diseases\nNeurodegenerative Diseases\nOphthalmic Diseases\nPain Management\nPsychiatric Disorders\nRare/Orphan Diseases\nRespiratory Diseases\nWound Care']
    column_name = ["service_categories_id"]
    if is_text_data(sample_data):  #Example of how keywords are extracted from sample data
        print("- Data is non-numeric. Extracting keywords...")
        keywords = extract_unique_words_advanced(sample_data)
        print(keywords)
    else :
        print("Data is numeric, no keywords to extract")
    print("\n")
    if is_text_data(column_name): #Example of how keywords are extracted from column_name
        print("- Data is non-numeric. Extracting keywords...")
        keywords = extract_unique_words_advanced(column_name)
        print(keywords)
    else :
        print("Data is numeric, no keywords to extract")

if __name__ == "__main__":
    main()