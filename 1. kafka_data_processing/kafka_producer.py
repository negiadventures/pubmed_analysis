# checks for any new file in the url and sends the link to kafka topic
import time
from urllib.request import urlopen

from bs4 import BeautifulSoup
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')
while True:
    processed_links = ''
    try:
        f = open('processed.txt', 'r')
        processed_links = f.read()
        f.close()
    except:
        pass
    base_url = 'https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/'
    base_url_update ='https://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/'
    html = urlopen(base_url)
    html_update = urlopen(base_url_update)

    soup = list(BeautifulSoup(html.read(), 'lxml').find_all('a'))
    soup.extend(list(BeautifulSoup(html_update.read(), 'lxml').find_all('a')))
    links = []
    print(soup)
    for a in soup:
        link = a.get('href')
        if link.endswith('.xml.gz') and link not in processed_links:
            links.append(link)
            producer.send('file_links', bytes(base_url + link, 'utf-8'))
    f = open('processed.txt', 'a+')
    new_links = '\n'.join(links) + '\n'
    f.write(new_links)
    f.close()
    producer.flush()
    time.sleep(10)
