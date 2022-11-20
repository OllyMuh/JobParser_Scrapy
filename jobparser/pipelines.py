# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from pymongo import MongoClient


class JobparserPipeline:
    def __init__(self):
        client = MongoClient('localhost', 27017)
        self.mongobase = client.vacancy3004

    def process_item(self, item, spider):
        if spider.name == 'hhru':
            item['min_salary'], item['max_salary'], item['currency'] = self.process_salary_hh(item['salary'])
            del item['salary']
        elif spider.name == 'sjobru':
            item['min_salary'], item['max_salary'], item['currency'] = self.process_salary_sj(item['salary'])
            del item['salary']

        collection = self.mongobase[spider.name]
        collection.insert_one(item)
        return item


    def process_salary_hh(self, salary):
        if salary:
            if 'от ' in salary:
                min_salary = float(salary[1].replace('\xa0', ''))
                if ' до ' in salary:
                    max_salary = float(salary[3].replace('\xa0', ''))
                    currency = salary[5]
                else:
                    max_salary = None
                    currency = salary[3]
            elif salary[0] == 'до ':
                min_salary = None
                max_salary = float(salary[1].replace('\xa0', ''))
                currency = salary[3]
            elif 'з/п не указана' in salary:
                min_salary = None
                max_salary = None
                currency = None
        return min_salary, max_salary, currency


    def process_salary_sj(self, salary):
        if salary:
            if 'до' in salary:
                salary = salary[2].replace('\xa0', ' ').split()
                max_salary = float(salary[0]+salary[1])
                min_salary = None
                currency = salary[2]
            elif 'от' in salary:
                salary = salary[2].replace('\xa0', ' ').split()
                min_salary = float(salary[0]+salary[1])
                max_salary = None
                currency = salary[2]
            elif salary[0].replace('\xa0', '').isnumeric():
                min_salary = float(salary[0].replace('\xa0', ''))
                max_salary = float(salary[1].replace('\xa0', ''))
                currency = salary[3]
            else:
                min_salary = None
                max_salary = None
                currency = None
        return min_salary, max_salary, currency
