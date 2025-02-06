# from itemadapter import ItemAdapter

# class BookscraperPipeline:
#     def process_item(self, item, spider):
#         return item


import mysql.connector
from itemadapter import ItemAdapter
import csv
import json
from pymongo import MongoClient
import logging
import os


class MariaDBPipeline:

    def __init__(self):
        self.create_connection()
        self.create_table()

    def create_connection(self):
        try:
            self.conn = mysql.connector.connect(
                user="root",
                password="root",
                host="127.0.0.1",
                database="bookscraper",
                charset="utf8mb4",
                collation="utf8mb4_general_ci",
            )
            self.curr = self.conn.cursor()
            print("Kết nối database thành công!")
        except mysql.connector.Error as err:
            print(f"Lỗi kết nối database: {err}")

    def create_table(self):
        try:

            self.curr.execute(
                """
                CREATE TABLE IF NOT EXISTS books(
                    title TEXT COLLATE utf8mb4_general_ci,
                    price TEXT COLLATE utf8mb4_general_ci,
                    upc VARCHAR(255) COLLATE utf8mb4_general_ci PRIMARY KEY,
                    image_url TEXT COLLATE utf8mb4_general_ci,
                    url TEXT COLLATE utf8mb4_general_ci
                )
            
                CHARACTER SET utf8mb4
            """
            )
            print("Tạo bảng thành công")

        except mysql.connector.Error as err:
            print(f"Lỗi tạo bảng: {err}")

    def process_item(self, item, spider):
        self.store_db(item)
        return item

    def store_db(self, item):
        try:
            sql = """
                        INSERT IGNORE INTO books (title, price, upc, image_url, url) 
                        VALUES (%s, %s, %s, %s, %s)
                    """
            self.curr.execute(
                sql,
                (
                    item.get("title"),
                    item.get("price"),
                    item.get("upc"),
                    item.get("image_url"),
                    item.get("url"),
                ),
            )
            self.conn.commit()

        except mysql.connector.Error as err:
            print(f"Lỗi khi lưu vào database: {err}")
        except Exception as e:
            print(f"Lỗi khác: {e}")

    def close_spider(self, spider):
        try:
            self.conn.close()
            print("Đóng kết nối database thành công!")
        except mysql.connector.Error as err:
            print(f"Lỗi đóng kết nối database: {err}")


class TXTPipeline:
    def __init__(self):
        os.makedirs("data", exist_ok=True)
        self.file = open("data/books.txt", "w")

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        self.file.write(
            f"{adapter['title']},{adapter['price']},{adapter['upc']},{adapter['image_url']},{adapter['url']}\n"
        )
        return item

    def close_spider(self, spider):
        self.file.close()


class CSVPipeline:
    def __init__(self):
        self.file = open("data/books.csv", "w")

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        self.file.write(
            f"{adapter['title']},{adapter['price']},{adapter['upc']},{adapter['image_url']},{adapter['url']}\n"
        )
        return item

    def close_spider(self, spider):
        self.file.close()


class JsonPipeline:
    def __init__(self):
        self.file = open("data/books.json", "w")

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        self.file.write(
            f"{adapter['title']},{adapter['price']},{adapter['upc']},{adapter['image_url']},{adapter['url']}\n"
        )
        return item

    def close_spider(self, spider):
        self.file.close()


class MongoDBPipeline:
    def __init__(self):
        self.client = pymongo.MongoClient("mongodb://localhost:27017/")
        self.db = self.client["bookscraper"]
        self.collection = self.db["books"]

    def process_item(self, item, spider):
        self.collection.insert_one(ItemAdapter(item).asdict())
        return item

    def close_spider(self, spider):
        self.client.close()


class MongoDBPipeline:
    def __init__(self, mongodb_settings):
        self.mongodb_settings = mongodb_settings

    @classmethod
    def from_crawler(cls, crawler):
        return cls(mongodb_settings=crawler.settings.get("MONGODB_SETTINGS"))

    def open_spider(self, spider):
        try:
            self.client = MongoClient(
                host=self.mongodb_settings["host"], port=self.mongodb_settings["port"]
            )
            self.db = self.client[self.mongodb_settings["db"]]
            self.collection = self.db["articles"]
            self.collection.create_index([("url", 1)], unique=True)
            logging.info("Successfully connected to MongoDB")
        except Exception as e:
            logging.error(f"Error connecting to MongoDB: {e}")
            raise

    def process_item(self, item, spider):
        try:
            item_dict = ItemAdapter(item).asdict()
            item_dict["updated_at"] = datetime.now()

            self.collection.update_one(
                {"url": item_dict["url"]}, {"$set": item_dict}, upsert=True
            )
            logging.info(f"Successfully saved item to MongoDB: {item_dict['url']}")
            return item
        except Exception as e:
            logging.error(f"Error saving to MongoDB: {e}")
            return item

    def close_spider(self, spider):
        try:
            self.client.close()
            logging.info("Closed MongoDB connection")
        except Exception as e:
            logging.error(f"Error closing MongoDB connection: {e}")
