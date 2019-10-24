# -*- coding: utf-8 -*-
import scrapy
from scrapy.linkextractors.lxmlhtml import LxmlLinkExtractor as LE
import re
from celeb.spiders.m_db_handler.db_handler import MysqlDbHandler
from celeb.spiders import data_handler
import time
from scrapy import signals

class WholecelebSpider(scrapy.Spider):
    name = 'wholeceleb'
    allowed_domains = ['wholecelebwiki.com']
    start_urls = ['http://wholecelebwiki.com/']
    celeb_form = re.compile("https:\/\/wholecelebwiki.com\/\w+-\w+\/")
    regex_form ="\/\w+-\w+"
    crawled_pages = set()
    celebs_corpus = data_handler.load_celebs_corpus()
    names_corpus = data_handler.load_names_corpus()
    db_instance = MysqlDbHandler.getInstance()
    last_time_record = time.time()
    last_celebs_corpus_size = len(celebs_corpus)

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """[After implementing this method the spider closed function will be launced before spider finishes its activity]
        
        Arguments:
            crawler {[type]} -- [description]
        
        Returns:
            [type] -- [description]
        """
        spider = super(WholecelebSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def parse(self, response):
        """Parse the links list."""
        links = LE().extract_links(response)
        if len(links) > 0:
            for link in links: 
                if link.url in self.crawled_pages:
                    continue
                else:
                    self.crawled_pages.add(link.url)
                if self.celeb_form.match(link.url): # If the link matches the celebrity link pattern
                    self.parse_url(link.url) # parse it and save in file
                yield scrapy.Request(link.url, callback=self.parse)

    def parse_url(self,url):
        """[The functions takes a url as input and reutrns capitalized space seperated celebrity name]
        
        Arguments:
            url {[str]} -- [url to parse]
        """
        
        celeb = re.search(self.regex_form,url)
        celeb = re.sub("-"," ",celeb.group()).title()
        self.save_celebs_to_corpus(celeb)


    def save_celebs_to_corpus(self,celeb):
        """[Add  the celebs to corpus. saves the corpus to cloud every data_handler.thresh_save_corpus seconds]
        
        Arguments:
            celeb {[str]} -- [The formmated celebrity string to be added to corpus]
        """
        if time.time()-self.last_time_record > data_handler.thresh_save_corpus:
            # Save only if the celebs corpus has new value
            if len(self.celebs_corpus) > self.last_celebs_corpus_size:
                print("SAVING TO CLOUD")
                data_handler.save_celebs_corpus(self.celebs_corpus)
                data_handler.save_names_corpus(self.names_corpus)
                self.last_celebs_corpus_size = len(self.celebs_corpus)
            self.last_time_record = time.time()
        try:
            self.celebs_corpus.add(celeb)
            print(" ##### Saving celeb : {} #######".format(celeb))
            names = celeb.split(" ")
            for name in names:
                if len(name)>1:
                    self.names_corpus.add(name)
        except Exception as e:
            print(e)
            pass

    def spider_closed(self):
        """[Function to activate when the spider is closed... saves the updated corpus to cloud]
        """
        print("SAVING BEFORE EXIT")
        data_handler.save_celebs_corpus(self.celebs_corpus)
        data_handler.save_names_corpus(self.names_corpus)