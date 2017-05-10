import sqlite3

from scrapy.exceptions import DropItem


class FilterPipeline(object):
    app_category_capacity = 500
    game_category_capacity = 100

    def __init__(self):
        self.all_package_name = []
        self.app_category_count = {}
        self.game_category_count = {}

    def process_item(self, item, spider):
        package_name = item['package_name']
        category = item['category']

        if package_name in self.all_package_name:
            raise DropItem("Drop duplicate item.")
        else:
            self.all_package_name.append(package_name)

        if 'application' in category:
            if category not in self.app_category_count:
                # init the count of this category
                self.app_category_count[category] = 0
            if self.app_category_count[category] >= self.app_category_capacity:
                raise DropItem("Drop overflow app item.")
            self.app_category_count[category] += 1
            return item

        if 'game' in category:
            if category not in self.game_category_count:
                # init the count of this category
                self.game_category_count[category] = 0
            if self.game_category_count[category] >= self.game_category_capacity:
                raise DropItem("Drop overflow game item.")
            self.game_category_count[category] += 1
            return item

        raise DropItem("Drop unknown item.")


class AppCategoryDbPipeline(object):
    db_log = False

    db_name = 'app_category.db'

    app_table_name = 'apps'
    app_table_col_key = 'app_key'
    app_table_col_category = 'category_index'
    app_table_col_package_name = 'package_name'

    category_table_name = 'categories'
    category_table_col_category = 'category_index'
    category_table_col_name = 'category'

    def __init__(self):
        self.category_id_map = {}
        self.next_category_id = 1
        self.conn = None
        self.cur = None

    def open_spider(self, spider):
        self.conn = sqlite3.connect(self.db_name)
        self.cur = self.conn.cursor()

        sql = 'DROP TABLE IF EXISTS %s' % self.app_table_name
        self.execute(sql)

        sql = 'CREATE TABLE %s (' \
              '%s INTEGER PRIMARY KEY NOT NULL UNIQUE, ' \
              '%s INTEGER NOT NULL,' \
              '%s TEXT NOT NULL' \
              ')' % (self.app_table_name, self.app_table_col_key, self.app_table_col_category, self.app_table_col_package_name)
        self.execute(sql)

        # sql = ('DROP TABLE IF EXISTS `category_new`')
        # self.execute(sql)
        #
        # sql = ('CREATE TABLE `category_new` ('
        #        '`category` INTEGER NOT NULL'
        #        ')')
        # self.execute(sql)
        #
        # sql = ('INSERT INTO `category_new`(`category`) VALUES(?)')
        # default_category = [self.entry_capacity] * self.entry_capacity
        # default_category = [(i,) for i in default_category]
        # self.executemany(sql, default_category)

    def close_spider(self, spider):
        sql = 'DROP TABLE IF EXISTS %s' % self.category_table_name
        self.execute(sql)

        sql = 'CREATE TABLE %s (' \
              '%s INTEGER PRIMARY KEY NOT NULL UNIQUE, ' \
              '%s TEXT NOT NULL' \
              ')' % (self.category_table_name, self.category_table_col_category, self.category_table_col_name)
        self.execute(sql)

        sql = 'INSERT INTO %s (%s, %s) VALUES(?, ?)' \
              % (self.category_table_name, self.category_table_col_name, self.category_table_col_category)
        self.executemany(sql, self.category_id_map.iteritems())

        self.conn.commit()
        self.conn.close()

    def process_item(self, item, spider):
        package_name = item['package_name']
        category = item['category']
        if category not in self.category_id_map:
            self.category_id_map[category] = self.next_category_id
            self.next_category_id += 1

        sql = 'INSERT OR IGNORE INTO %s (%s, %s, %s) VALUES(?, ?, ?)' \
              % (self.app_table_name, self.app_table_col_key, self.app_table_col_category, self.app_table_col_package_name)
        pkg_key = AppCategoryDbPipeline.fnv32a(package_name)
        category_id = self.category_id_map[category]
        self.execute(sql, (pkg_key, category_id, package_name))

        return item

    def execute(self, sql, params=None):
        if self.db_log:
            print '[SQL]:', sql, params
        if params:
            self.cur.execute(sql, params)
        else:
            self.cur.execute(sql)

    def executemany(self, sql, params=None):
        if self.db_log:
            print '[SQL]:', sql, params
        if params:
            self.cur.executemany(sql, params)
        else:
            self.cur.executemany(sql)

    @classmethod
    def fnv32a(cls, str):
        hval = 0x811c9dc5
        fnv_32_prime = 0x01000193
        uint32_max = 2 ** 32
        for s in str:
            hval = hval ^ ord(s)
            hval = (hval * fnv_32_prime) % uint32_max
        return hval
