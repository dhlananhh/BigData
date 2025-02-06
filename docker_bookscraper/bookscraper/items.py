import scrapy

class BookItem (scrapy.Item):
    title = scrapy.Field()
    price = scrapy.Field()
    upc = scrapy.Field()
    image_url = scrapy.Field()
    url = scrapy.Field()