import scrapy
from scrapy.http import HtmlResponse
from jobparser.items import JobparserItem


class SjobruSpider(scrapy.Spider):
    name = 'sjobru'
    allowed_domains = ['superjob.ru']
    start_urls = ['https://www.superjob.ru/vacancy/search/?keywords=big%20data&geo%5Bt%5D%5B0%5D=4',
                  'https://www.superjob.ru/vacancy/search/?keywords=python&geo%5Bt%5D%5B0%5D=4']


    def parse(self, response: HtmlResponse):
        # response.status
        next_page = response.xpath("//a[contains(@class, 'dalshe')]/@href").get()
        if next_page:
            yield response.follow(next_page, callback=self.parse)

        links = response.xpath("//span[@class='-gENC _1TcZY Bbtm8']//@href").getall()
        for link in links:
            yield response.follow(link, callback=self.vacancy_parse)

    def vacancy_parse(self, response: HtmlResponse):
        name = response.css('h1::text').get()
        salary = response.xpath('//span[@class="_2eYAG -gENC _1TcZY dAWx1"]/text()').getall()
        url = response.url
        yield JobparserItem(name=name, salary=salary, url=url)
