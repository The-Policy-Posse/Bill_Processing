# -*- coding: utf-8 -*-
"""
Created on Mon Oct 21 22:19:00 2024

@author: dforc

## Description:

    This script processes a dataset of congressional bills, extracting key information 
    from columns containing JSON-like strings (e.g., sponsors, cosponsors, subjects, summaries). 
    It uses parallel processing with Swifter to apply functions efficiently and extracts specific 
    information for each segment, including sponsor details, summaries count, cosponsor information, 
    subject categorization, and the latest bill summary. The final results are saved into a new CSV file.

### Segment Overview:

    1. **Segment 1: Load Dataset**  

    2. **Segment 2: Extract sponsor information**  

    3. **Segment 3: Extract summaries count**  

    4. **Segment 4: Extract cosponsor information**  

    5. **Segment 5: Extract subject information**  

    6. **Segment 6: Extract latest summary text**  

    7. **Segment 7: Save DataFrame to CSV**  
"""

import pandas as pd
import json
import swifter
import ast

########################################################################
### Segment 1: Load Dataset
########################################################################

## Load the dataset from the CSV file
df = pd.read_csv('bill_info_filled.csv')

########################################################################
### Segment 2: Extract sponsor information
########################################################################

## Function to extract sponsor information from 'sponsors' column
def get_sponsor_info(sponsors_str):
    """
    Extract sponsor information from the 'sponsors' column, returning bioguide IDs, 
    sponsor names, and the sponsor count.
    """
    if isinstance(sponsors_str, str):
        try:
            data = ast.literal_eval(sponsors_str)
        except (SyntaxError, ValueError):
            return pd.Series(['NA', 'NA', 0])
        
        sponsors = []
        bioguideIds = []
        sponsor_count = 0
        
        if 'bill' in data and 'sponsors' in data['bill']:
            sponsor_list = data['bill']['sponsors']
            sponsor_count = len(sponsor_list)
            for sponsor in sponsor_list:
                first_name = sponsor.get('firstName', '')
                last_name = sponsor.get('lastName', '')
                full_name = f"{first_name} {last_name}".strip()
                sponsors.append(full_name)
                
                bioguideId = sponsor.get('bioguideId', '')
                bioguideIds.append(bioguideId)
                
            sponsor_names_str = '; '.join(sponsors)
            bioguideIds_str = '; '.join(bioguideIds)
            return pd.Series([bioguideIds_str, sponsor_names_str, sponsor_count])
    return pd.Series(['NA', 'NA', 0])

## Apply the function using swifter for parallel processing
df[['sponsor_bioguideIds', 'sponsor_names', 'sponsor_count']] = df['sponsors'].swifter.apply(get_sponsor_info)

## Handle missing data
df['sponsor_bioguideIds'] = df['sponsor_bioguideIds'].fillna('NA')
df['sponsor_names'] = df['sponsor_names'].fillna('NA')
df['sponsor_count'] = df['sponsor_count'].fillna(0).astype(int)

########################################################################
### Segment 3: Extract summaries count
########################################################################

## Function to extract 'count' from the 'summaries' column
def get_summaries_count(summaries_str):
    """
    Extract the 'count' from the 'summaries' column, which contains information about 
    the number of summaries available for a bill.
    """
    if isinstance(summaries_str, str):
        try:
            data = ast.literal_eval(summaries_str)
        except (SyntaxError, ValueError):
            return 0
        if 'pagination' in data and 'count' in data['pagination']:
            return data['pagination']['count']
    return 0

## Apply the function using swifter
df['summaries_count'] = df['summaries'].swifter.apply(get_summaries_count)

## Handle missing data
df['summaries_count'] = df['summaries_count'].fillna(0).astype(int)



########################################################################
### Segment 4: Extract cosponsor information
########################################################################

## Function to extract cosponsor information from 'cosponsors' column
def get_cosponsor_info(cosponsors_str):
    """
    Extract cosponsor information from the 'cosponsors' column, returning cosponsor 
    bioguide IDs, names, and the cosponsor count.
    """
    if isinstance(cosponsors_str, str):
        try:
            data = ast.literal_eval(cosponsors_str)
        except (SyntaxError, ValueError):
            return pd.Series([0, 'NA', 'NA'])
        
        cosponsor_count = 0
        cosponsor_bioguideIds = []
        cosponsor_names = []
        
        if 'cosponsors' in data and isinstance(data['cosponsors'], list):
            cosponsor_list = data['cosponsors']
            cosponsor_count = len(cosponsor_list)
            for cosponsor in cosponsor_list:
                bioguideId = cosponsor.get('bioguideId', '')
                first_name = cosponsor.get('firstName', '')
                last_name = cosponsor.get('lastName', '')
                full_name = f"{first_name} {last_name}".strip()
                cosponsor_bioguideIds.append(bioguideId)
                cosponsor_names.append(full_name)
                
            cosponsor_bioguideIds_str = '; '.join(cosponsor_bioguideIds)
            cosponsor_names_str = '; '.join(cosponsor_names)
            return pd.Series([cosponsor_count, cosponsor_bioguideIds_str, cosponsor_names_str])
    return pd.Series([0, 'NA', 'NA'])

## Apply the function using swifter
df[['cosponsor_count', 'cosponsor_bioguideIds', 'cosponsor_names']] = df['cosponsors'].swifter.apply(get_cosponsor_info)

# Handle missing data
df['cosponsor_count'] = df['cosponsor_count'].fillna(0).astype(int)
df['cosponsor_bioguideIds'] = df['cosponsor_bioguideIds'].fillna('NA')
df['cosponsor_names'] = df['cosponsor_names'].fillna('NA')

########################################################################
### Segment 5: Extract subject information
########################################################################



## Function to extract subject and policy area information from 'subjects' column
def get_subject_info(subjects_str):
    """
    Extract subject and policy area information from the 'subjects' column, returning 
    the subject count, subject texts, policy area count, and policy area texts.
    """
    if isinstance(subjects_str, str):
        try:
            data = ast.literal_eval(subjects_str)
        except (SyntaxError, ValueError):
            return pd.Series([0, 'NA', 0, 'NA'])
        
        subject_texts = []
        subject_count = 0
        policy_area_texts = []
        policy_area_count = 0

        if 'subjects' in data:
            subjects_data = data['subjects']
            if 'legislativeSubjects' in subjects_data and subjects_data['legislativeSubjects']:
                subjects_list = subjects_data['legislativeSubjects']
                subject_count = len(subjects_list)
                for subject in subjects_list:
                    subject_name = subject.get('name', '')
                    subject_texts.append(subject_name)
                    
            if 'policyArea' in subjects_data:
                policy_area = subjects_data['policyArea']
                if isinstance(policy_area, dict):
                    policy_area_name = policy_area.get('name', '')
                    if policy_area_name:
                        policy_area_texts.append(policy_area_name)
                        policy_area_count = 1
                elif isinstance(policy_area, list):
                    policy_area_names = [pa.get('name', '') for pa in policy_area if pa.get('name', '')]
                    policy_area_texts.extend(policy_area_names)
                    policy_area_count = len(policy_area_names)

        subject_texts_str = '; '.join(subject_texts) if subject_texts else 'NA'
        policy_area_texts_str = '; '.join(policy_area_texts) if policy_area_texts else 'NA'
        return pd.Series([subject_count, subject_texts_str, policy_area_count, policy_area_texts_str])
    return pd.Series([0, 'NA', 0, 'NA'])

## Apply the function using swifter
df[['subject_count', 'subject_texts', 'policy_area_count', 'policy_area_texts']] = df['subjects'].swifter.apply(get_subject_info)

## Handle missing data
df['subject_count'] = df['subject_count'].fillna(0).astype(int)
df['subject_texts'] = df['subject_texts'].fillna('NA')
df['policy_area_count'] = df['policy_area_count'].fillna(0).astype(int)
df['policy_area_texts'] = df['policy_area_texts'].fillna('NA')




########################################################################
### Segment 6: Extract latest summary text
########################################################################

## Function to extract the latest summary text from 'summaries' column
def get_latest_summary_text(summaries_str):
    """
    Extract the latest summary text from the 'summaries' column, sorting the summaries 
    by 'updateDate' or 'actionDate' in descending order to get the most recent one.
    """
    if isinstance(summaries_str, str):
        try:
            data = ast.literal_eval(summaries_str)
        except (SyntaxError, ValueError):
            return 'NA'
        
        if 'summaries' in data and isinstance(data['summaries'], list):
            summaries_list = data['summaries']
            if not summaries_list:
                return 'NA'
            
            # Sort summaries by 'updateDate' or 'actionDate', descending
            def get_date(summary):
                return summary.get('updateDate', '') or summary.get('actionDate', '')
            
            summaries_list_sorted = sorted(summaries_list, key=get_date, reverse=True)
            latest_summary = summaries_list_sorted[0]
            latest_text = latest_summary.get('text', 'NA')
            return latest_text
    return 'NA'

## Apply the function using swifter
df['latest_summary_text'] = df['summaries'].swifter.apply(get_latest_summary_text)

## Handle missing data
df['latest_summary_text'] = df['latest_summary_text'].fillna('NA')

########################################################################
### Segment 7: Save DataFrame to CSV
########################################################################

## Save the updated dataframe to a new CSV file
df.to_csv('bill_info_with_extracted_data.csv', index=False)







