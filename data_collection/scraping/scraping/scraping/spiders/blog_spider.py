import scrapy

class BlogSpider(scrapy.Spider):
    name = "blog_spider"
    allowed_domains = ["example.com"]  
    start_urls = [
        # 'http://example.com/blog', 
    ]

    def parse(self, response):
        # Loop through blog articles on the page
        for article in response.css('div.blog-post'):
            yield {
                'title': article.css('h2.blog-post-title::text').get(),
                'content': article.css('div.blog-post-content').get(),
                'date': article.css('span.blog-post-date::text').get(),
                'author': article.css('span.blog-post-author::text').get(),
                'source': response.url
            }
        
        # Follow pagination links
        next_page = response.css('a.next-page::attr(href)').get()
        if next_page is not None:
            yield response.follow(next_page, self.parse)
