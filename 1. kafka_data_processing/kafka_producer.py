# checks for any new file in the url and sends the link to kafka topic
import time
from urllib.request import urlopen

from bs4 import BeautifulSoup
from kafka import KafkaProducer

# kafka consumer, that puts any new message to the topic based on new files on server.
producer = KafkaProducer(bootstrap_servers='localhost:9092')
while True:
    processed_links = ''
    try:
        # processed.txt maintains a list of files that are already processed to avoid reprocessing
        f = open('processed.txt', 'r')
        processed_links = f.read()
        f.close()
    except:
        pass
    # annual baseline data files
    base_url = 'https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/'
    # daily datafiles
    base_url_update = 'https://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/'
    html = urlopen(base_url)
    html_update = urlopen(base_url_update)
    # getting all file links from file location
    soup = list(BeautifulSoup(html.read(), 'lxml').find_all('a'))
    soup.extend(list(BeautifulSoup(html_update.read(), 'lxml').find_all('a')))
    links = []
    print(soup)
    for a in soup:
        link = a.get('href')
        # if already processed, skip files, otherwise send message to topic to be processed by consumer
        if link.endswith('.xml.gz') and link not in processed_links:
            links.append(link)
            producer.send('file_links', bytes(base_url + link, 'utf-8'))
    f = open('processed.txt', 'a+')
    new_links = '\n'.join(links) + '\n'
    f.write(new_links)
    f.close()
    producer.flush()
    # polls every 10 seconds for new files to process
    time.sleep(10)
