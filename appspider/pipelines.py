import sqlite3

from scrapy.exceptions import DropItem


class FilterPipeline(object):
    max_size = 200
    categories_count = {}
    all_package_name = []

    def process_item(self, item, spider):
        if item['package_name'] in self.all_package_name:
            raise DropItem("Drop duplicate item.")
        else:
            self.all_package_name.append(item['package_name'])

        if item['category'] not in self.categories_count:
            self.categories_count[item['category']] = 0

        if self.categories_count[item['category']] >= self.max_size:
            raise DropItem("Drop overflow item.")

        self.categories_count[item['category']] += 1
        return item


class SqlitePipeline(object):
    db_name = "app_category.db"

    def open_spider(self, spider):
        self.category_type = {}
        self.category_index = 0

        self.conn = sqlite3.connect(self.db_name)
        self.cur = self.conn.cursor()

        sql = ('DROP TABLE IF EXISTS `app_category`')
        self.execute(sql)

        sql = ('CREATE TABLE `app_category` ('
               '`package_name` INTEGER PRIMARY KEY NOT NULL UNIQUE,'
               '`category` INTEGER NOT NULL'
               ')')
        self.execute(sql)

    def close_spider(self, spider):
        sql = ('DROP TABLE IF EXISTS `category_map`')
        self.execute(sql)

        sql = ('CREATE TABLE `category_map` ('
               '`index` INTEGER PRIMARY KEY NOT NULL UNIQUE,'
               '`category` TEXT NOT NULL'
               ')')
        self.execute(sql)

        sql = ('INSERT OR IGNORE INTO `category_map`(`category`, `index`) VALUES(?, ?)')
        self.executemany(sql, self.category_type.items())

        self.conn.commit()
        self.conn.close()

    def process_item(self, item, spider):
        sql = ('INSERT OR IGNORE INTO `app_category`(`package_name`, `category`) VALUES(?, ?)')
        hash_pkg = self.fnv32a(item['package_name'])

        if item['category'] not in self.category_type:
            self.category_index += 1
            self.category_type[item['category']] = self.category_index
        category_index = self.category_type[item['category']]
        # hash_cat = self.fnv32a(item['category'])
        self.execute(sql, (hash_pkg, category_index))
        return item

    def execute(self, sql, params=None, log=False):
        if (log):
            print '[SQL]:', sql, params

        if params:
            self.cur.execute(sql, params)
        else:
            self.cur.execute(sql)

    def executemany(self, sql, params=None, log=False):
        if (log):
            print '[SQL]:', sql, params

        if params:
            self.cur.executemany(sql, params)
        else:
            self.cur.execute(sql)

    def fnv32a(self, str):
        hval = 0x811c9dc5
        fnv_32_prime = 0x01000193
        uint32_max = 2 ** 32
        for s in str:
            hval = hval ^ ord(s)
            hval = (hval * fnv_32_prime) % uint32_max
        return hval


class SqliteDebugPipeline(object):
    db_name = "app_category_debug.db"

    def open_spider(self, spider):
        self.category_type = {}
        self.category_index = 0

        self.conn = sqlite3.connect(self.db_name)
        self.cur = self.conn.cursor()

        sql = ('DROP TABLE IF EXISTS `app_category`')
        self.execute(sql)

        sql = ('CREATE TABLE `app_category` ('
               '`package_name` INTEGER PRIMARY KEY NOT NULL UNIQUE,'
               '`category` INTEGER NOT NULL,'
               '`package_name_d` TEXT NOT NULL'
               ')')
        self.execute(sql)

    def close_spider(self, spider):
        sql = ('DROP TABLE IF EXISTS `category_map`')
        self.execute(sql)

        sql = ('CREATE TABLE `category_map` ('
               '`index` INTEGER PRIMARY KEY NOT NULL UNIQUE,'
               '`category` TEXT NOT NULL'
               ')')
        self.execute(sql)

        sql = ('INSERT OR IGNORE INTO `category_map`(`category`, `index`) VALUES(?, ?)')
        self.executemany(sql, self.category_type.items())

        self.conn.commit()
        self.conn.close()

    def process_item(self, item, spider):
        sql = ('INSERT OR IGNORE INTO `app_category`(`package_name`, `category`, `package_name_d`) VALUES(?, ?, ?)')
        package_name = item['package_name']
        hash_pkg = self.fnv32a(package_name)

        if item['category'] not in self.category_type:
            self.category_index += 1
            self.category_type[item['category']] = self.category_index
        category_index = self.category_type[item['category']]
        # hash_cat = self.fnv32a(item['category'])
        self.execute(sql, (hash_pkg, category_index, package_name))
        return item

    def execute(self, sql, params=None, log=False):
        if (log):
            print '[SQL]:', sql, params

        if params:
            self.cur.execute(sql, params)
        else:
            self.cur.execute(sql)

    def executemany(self, sql, params=None, log=False):
        if (log):
            print '[SQL]:', sql, params

        if params:
            self.cur.executemany(sql, params)
        else:
            self.cur.execute(sql)

    def fnv32a(self, str):
        hval = 0x811c9dc5
        fnv_32_prime = 0x01000193
        uint32_max = 2 ** 32
        for s in str:
            hval = hval ^ ord(s)
            hval = (hval * fnv_32_prime) % uint32_max
        return hval
