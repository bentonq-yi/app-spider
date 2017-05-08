from scrapy import cmdline
cmdline.execute("scrapy crawl annie -o annie.db -t sqlite".split())