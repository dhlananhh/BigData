# from itemadapter import ItemAdapter

# class BookscraperPipeline:
#     def process_item(self, item, spider):
#         return item


import mysql.connector
from itemadapter import ItemAdapter
import csv
import json


class MariaDBPipeline:

    def __init__(self):
        self.create_connection()
        self.create_table()

    def create_connection(self):
        try:
            self.conn = mysql.connector.connect (
                user='root',  
                password='root',  
                host='127.0.0.1', 
                database='bookscraper',
                charset='utf8mb4',
                collation='utf8mb4_general_ci',
            )
            self.curr = self.conn.cursor()
            print("Kết nối database thành công!") 
        except mysql.connector.Error as err:
            print(f"Lỗi kết nối database: {err}")



    def create_table(self):
        try:

            self.curr.execute("""
                CREATE TABLE IF NOT EXISTS books(
                    title TEXT COLLATE utf8mb4_general_ci,
                    price TEXT COLLATE utf8mb4_general_ci,
                    upc VARCHAR(255) COLLATE utf8mb4_general_ci PRIMARY KEY,
                    image_url TEXT COLLATE utf8mb4_general_ci,
                    url TEXT COLLATE utf8mb4_general_ci
                )
            
                CHARACTER SET utf8mb4
            """)
            print("Tạo bảng thành công") 

        except mysql.connector.Error as err:
            print(f"Lỗi tạo bảng: {err}")


    def process_item(self, item, spider):
        self.store_db(item)
        return item

    def store_db(self, item):
        try:
            sql =   """
                        INSERT IGNORE INTO books (title, price, upc, image_url, url) 
                        VALUES (%s, %s, %s, %s, %s)
                    """
            self.curr.execute(sql, (
                item.get("title"), item.get("price"),item.get("upc"), item.get("image_url"),item.get("url")
            ))
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
            
class CSVPipeline:
    def __init__(self):
        self.file = open("data/books.csv", "w")

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        self.file.write(f"{adapter['title']},{adapter['price']},{adapter['upc']},{adapter['image_url']},{adapter['url']}\n")
        return item

    def close_spider(self, spider):
        self.file.close()
        
class JsonPipeline:
    def __init__(self):
        self.file = open("data/books.json", "w")

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        self.file.write(f"{adapter['title']},{adapter['price']},{adapter['upc']},{adapter['image_url']},{adapter['url']}\n")
        return item

    def close_spider(self, spider):
        self.file.close()