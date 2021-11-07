# https://towardsdatascience.com/quickstart-apache-kafka-kafka-python-e8356bec94
# https://www.apache.org/dyn/closer.cgi?path=/kafka/3.0.0/kafka_2.13-3.0.0.tgz
# listens to kafka topic and download, extracts, transforms data to csv, dump it to two files
import gzip
import io
import xml.etree.ElementTree as ET
from urllib.request import urlopen

import pandas as pd
from kafka import KafkaConsumer


def process_xml_csv(file_url):
    '''
    Reads an xml.gz file from the url, converts into dataframe by extracting required fields.
    :param file_url: file path (url) where it exists
    :return: dataframe parsed from xml
    '''
    tree = ET.parse(gzip.GzipFile(fileobj=io.BytesIO(urlopen(file_url).read())))
    pd.set_option('display.max_columns', None)
    dict_list = []
    for pubmed_article in tree.iter('PubmedArticle'):
        data = dict()
        for medline_citation in pubmed_article.iter('MedlineCitation'):
            data['pmid'] = medline_citation.find('PMID').text
            data['date_completed'] = ''
            data['date_revised'] = ''
            for date_completed in medline_citation.iter('DateCompleted'):
                data['date_completed'] = date_completed.find('Year').text + '-' + date_completed.find('Month').text + '-' + date_completed.find('Day').text
            for date_revised in medline_citation.iter('DateRevised'):
                data['date_revised'] = date_revised.find('Year').text + '-' + date_revised.find('Month').text + '-' + date_revised.find('Day').text
            for article in medline_citation.iter('Article'):
                for journal in article.iter('Journal'):
                    try:
                        data['journal_issn'] = journal.find('ISSN').text
                    except:
                        # no issn found
                        data['journal_issn'] = 0
                    for journal_issue in journal.iter('JournalIssue'):
                        try:
                            data['journal_volume'] = journal_issue.find('Volume').text
                            data['journal_issue'] = journal_issue.find('Issue').text
                            for journal_pub in journal_issue.iter('PubDate'):
                                try:
                                    data['journal_pub_date'] = journal_pub.find('Year').text + '-' + journal_pub.find('Month').text
                                except:
                                    # different date structure
                                    yr = journal_pub.find('MedlineDate').text.split(' ')[0]
                                    mon = journal_pub.find('MedlineDate').text.split(' ')[1].split('-')[1]
                                    data['journal_pub_date'] = yr + '-' + mon + '-01'
                        except:
                            # no volume found
                            data['journal_volume'] = 0
                            data['journal_issue'] = 0
                            data['journal_pub_date'] = '1970-Jan-01'

                    data['journal_title'] = journal.find('Title').text
                    try:
                        data['journal_iso_abbr'] = journal.find('ISOAbbreviation').text
                    except:
                        data['journal_iso_abbr'] = ''
                try:
                    data['article_title'] = article.find('ArticleTitle').text.replace('"', '')
                except:
                    data['article_title'] = ''
                for author_list in article.iter('AuthorList'):
                    authors = ''
                    for author in author_list.iter('Author'):
                        try:
                            authors += author.find('ForeName').text + ' ' + author.find('LastName').text + ' ' + author.find('Initials').text + ','
                        except:
                            # no authors
                            pass
                    authors = authors.strip(',')
                    data['authors'] = authors
                data['language'] = article.find('Language').text
                data['pub_types'] = ''
                for pub_type_list in article.iter('PublicationTypeList'):
                    # pub_types = ''
                    # for pub_type in pub_type_list.iter('PublicationType'):
                    #     pub_types += pub_type.find('PublicationType').text + ','
                    # pub_types=pub_types.strip(',')
                    data['pub_types'] = pub_type_list.find('PublicationType').text
            data['country'] = ''
            data['medline_ta'] = ''
            data['nlm_unique_id'] = 0
            data['issn_linking'] = 0
            for medline_journal_info in medline_citation.iter('MedlineJournalInfo'):
                try:
                    data['country'] = medline_journal_info.find('Country').text
                except:
                    pass
                data['medline_ta'] = medline_journal_info.find('MedlineTA').text
                data['nlm_unique_id'] = medline_journal_info.find('NlmUniqueID').text
                try:
                    data['issn_linking'] = medline_journal_info.find('ISSNLinking').text
                except:
                    # no issn_linking found
                    data['issn_linking'] = 0
                # data['citation_subset'] = medline_citation.find('CitationSubset').text
            data['mesh_headings'] = ''
            for mesh_heading_list in medline_citation.iter('MeshHeadingList'):
                mesh_headings = ''
                for mesh_heading in mesh_heading_list.iter('MeshHeading'):
                    mesh_headings += mesh_heading.find('DescriptorName').text + ','
                mesh_headings = mesh_headings.strip(',')
                data['mesh_headings'] = mesh_headings
        for pubmed_data in pubmed_article.iter('PubmedData'):
            data['pubmed_pub_date'] = ''
            for history in pubmed_data.iter('History'):
                for pubmed_pub_date in history.iter('PubMedPubDate'):
                    data['pubmed_pub_date'] = pubmed_pub_date.find('Year').text + '-' + pubmed_pub_date.find('Month').text + '-' + pubmed_pub_date.find('Day').text
            data['publication_status'] = pubmed_data.find('PublicationStatus').text
            for article_id_list in pubmed_data.iter('ArticleIdList'):
                data['article_id'] = article_id_list.find('ArticleId').text
            reference_ids = ''
            for reference_id_list in pubmed_data.iter('ReferenceList'):
                for reference in reference_id_list.iter('Reference'):
                    for articleIdList in reference.iter('ArticleIdList'):
                        reference_ids += articleIdList.find('ArticleId').text + ','
            reference_ids = reference_ids.strip(',')
            data['reference_ids'] = reference_ids
        dict_list.append(data)
    return pd.DataFrame(dict_list)


def data_graph(df):
    '''
    Generates edge network dataframe from using PMID (pubmed id) and reference_ids(comma separated pubmed ids of other citations) as Source and Target nodes respectively.
    :param df: dataframe to process
    :return: edge network dataframe
    '''
    df = df[['pmid', 'reference_ids']]
    df = df[df['reference_ids'] != '']
    df = df.drop('reference_ids', axis=1).join(df['reference_ids'].str.split(',', expand=True).stack().reset_index(drop=True, level=1).rename('Target')).apply(lambda x: x.str.strip())
    df = df.reset_index(drop=True)
    df.rename(columns={'pmid': 'Source'}, inplace=True)
    return df


# kafka consumer, that listens file_links topic for any new message
consumer = KafkaConsumer('file_links', bootstrap_servers=['localhost:9092'], group_id='grp1')

for msg in consumer:
    # processes each message in the topic
    url = msg.value.decode('UTF-8')
    file_name = url.split('/')[len(url.split('/')) - 1].strip('.xml.gz')
    # converts xml to csv
    df = process_xml_csv(url)
    df.to_csv('csv_data/' + file_name + '.csv', header=True, index=False)
    # converts data to graph - edge network
    df_graph = data_graph(df)
    df_graph.to_csv('graph_data/' + file_name + '_graph.csv', header=True, index=False)
    # Clean graph data
    df = pd.read_csv('graph_data/' + file_name + '_graph.csv')
    df = df[df.Target.apply(lambda x: str(x).isnumeric())]
    df.to_csv('graph_data/' + file_name + '_graph.csv', header=True, index=False)
    del df
