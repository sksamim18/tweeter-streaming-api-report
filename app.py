import re
import json
import threading

import datetime
import requests
from urllib import parse
from collections import defaultdict

from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener


comsumer_key = 'yQQ3veBXgTVXk9LIstse8ku3k'
consumer_secret = 'Mgw5irsKMNin9lSfd6CcVvNZpS90CMTUNzu7EI5sOgsLIQUuiu'

access_token = '1076554978276327426-l8HKJIEjjdstHXqj213WztiIt3XtFO'
access_secret = 'QUqkQcvS3ccKoTEtFnQKDgiqaWy275WpczHSxH4gwz6sk'

MESSAGE = 'DATA FOR TIME SLOT: {} \n\n'

file = open('stopwords.txt', 'r')
content = file.read()
pattern = re.compile("<li>[a-zA-Z']+</li>")

STOPWORDS = set(pattern.findall(content))

NLTK_WORDS = ['ourselves', 'hers', 'between', 'yourself', 'but', 'again',
              'there', 'about', 'once', 'during', 'out', 'very', 'having',
              'with', 'they', 'own', 'an', 'be', 'some', 'for', 'do', 'its',
              'yours', 'such', 'into', 'of', 'most', 'itself', 'other', 'off',
              'is', 's', 'am', 'or', 'who', 'as', 'from', 'him', 'each', 'the',
              'themselves', 'until', 'below', 'are', 'we', 'these', 'your',
              'his', 'through', 'don', 'nor', 'me', 'were', 'her', 'more',
              'himself', 'this', 'down', 'should', 'our', 'their', 'while',
              'above', 'both', 'up', 'to', 'ours', 'had', 'she', 'all', 'no',
              'when', 'at', 'any', 'before', 'them', 'same', 'and', 'been',
              'have', 'in', 'will', 'on', 'does', 'yourselves', 'then', 'that',
              'because', 'what', 'over', 'why', 'so', 'can', 'did', 'not',
              'now', 'under', 'he', 'you', 'herself', 'has', 'just', 'where',
              'too', 'only', 'myself', 'which', 'those', 'i', 'after',
              'few', 'whom', 't', 'being', 'if', 'theirs', 'my', 'against',
              'a', 'by', 'doing', 'it', 'how', 'further', 'was', 'here', 'than'
              'https', 'http']

STOPWORDS.update(NLTK_WORDS)


class GenerateReport(StreamListener):

    def __init__(self):

        self.track_time = process_start_time
        self.tweets = list()
        self.time_slot = str(0)

        super().__init__()

    def get_report(self):
        print(MESSAGE.format(self.time_slot))

        users, links = defaultdict(int), list()
        status_list, data = list(), {}

        for tweet in self.tweets:
            name = tweet.get('user', {}).get('screen_name')
            link = tweet.get('entities', {}).get('urls', [])
            text = tweet.get('text')

            if name:
                users[name] += 1
            if text:
                status_list.append(text)
            if link:
                url = link[0].get('expanded_url')
                if url:
                    links.append(url)

        data['users'] = users
        data['links'] = links
        data['status_list'] = status_list

        self.log_report(data)

    def get_cleaned_links(self, links):
        links_dict = defaultdict()

        for link in set(links):
            res = requests.get(link)
            links_dict[link] = res.url

        return links_dict

    def log_report(self, data):
        text_dict = defaultdict(int)
        domains_dict = defaultdict(int)

        for status in data.get('status_list'):
            try:
                p = re.compile("\s*([a-zA-Z]+)\s*")
                words = p.findall(status)
                words = [word.lower() for word in words if len(word) > 2]
                filtered_words = set(words) - STOPWORDS
                for word in filtered_words:
                    text_dict[word] += 1
            except:
                pass

        cleaned_links = self.get_cleaned_links(data.get('links'))

        for link in data.get('links'):
            try:
                link = cleaned_links.get(link)
                url_details = parse.urlparse(link)
                host = url_details.netloc
                if host:
                    domains_dict[host] += 1
            except:
                pass

        domains = sorted(
            domains_dict.items(), key=lambda x: x[1], reverse=True)[:10]
        word_list = sorted(
            text_dict.items(), key=lambda x: x[1], reverse=True)[:10]

        # User Report Logging
        print('*******USER REPORT********')
        user_report = [str(name) + ': ' + str(
            count) for name, count in data.get('users').items()]
        print('USER COUNT: ')
        print('\t'.join(user_report))
        print('\n\n\n')

        # Links report Logging
        print('*******LINKS REPORT********')
        links_report = [str(domain[0]) + ': ' + str(
            domain[1]) for domain in domains]
        print('UNIQUE LINKS: ')
        print('\n'.join(cleaned_links.keys()))
        print('\n')
        print('TOP DOMAINS: ')
        print('\t'.join(links_report))
        print('\n\n\n')

        # Content report Logging
        print('*******CONTENT REPORT********')
        content_report = [str(text[0]) + ': ' + str(
            text[1]) for text in word_list]
        print('UNIQUE WORDS: ')
        print('\t'.join(text_dict.keys()))
        print('\n')
        print('MOST USED WORDS: ')
        print('\t'.join(content_report))
        print('\n\n\n')

    def on_data(self, data):
        if self.time_slot == str(5):
            return

        data = json.loads(data)
        current_time = datetime.datetime.now()
        diff = (current_time - self.track_time).total_seconds()
        diff = (diff / 60) > 1

        if diff:
            self.time_slot = str(int(self.time_slot) + 1)
            self.track_time = current_time
            t1 = threading.Thread(target=self.get_report)
            t1.start()

        self.tweets.append(data)

    def on_error(self, status):
        print(status)
        print('Something definately went wrong')


if __name__ == '__main__':

    keyword = input('Enter a keyword: ')
    while not keyword:
        keyword = input('Enter a keyword: ')

    auth = OAuthHandler(comsumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)

    process_start_time = datetime.datetime.now()

    twitter_stream = Stream(auth, GenerateReport())
    twitter_stream.filter(track=[keyword])
