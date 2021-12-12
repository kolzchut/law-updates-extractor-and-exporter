import sqlite3


class Database:
    booklet_types = {
        "law": 1,
        "takana": 2
    }

    def __enter__(self):
        self.conn = sqlite3.connect('kzdb.sqlite')
        self.conn.row_factory = sqlite3.Row
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()

    def insert_item(self, item_type, item):
        with self.conn:
            self.conn.execute(f'''INSERT INTO booklet 
                (file_name, extension, booklet_number, number_of_pages, description,
                booklet_creation_date, modify_date, published_date, booklet_type,
                display_name, foreign_year)
                VALUES (:file_name, :extension, :booklet_number, :number_of_pages, :description,
                :creation_date, :modify_date, :published_date, {item_type}, :display_name,
                :foreign_year)''', item)

    def insert_takana(self, takana):
        self.insert_item(self.booklet_types['takana'], takana)

    def insert_law(self, law):
        self.insert_item(self.booklet_types['law'], law)

    def get_last_of_type(self, item_type):
        with self.conn:
            return self.conn.execute(f'''SELECT id, file_name, booklet_number, booklet_creation_date
            FROM booklet WHERE booklet_type = {item_type} ORDER BY id DESC LIMIT 1''').fetchone()

    def get_last_law(self):
        return self.get_last_of_type(self.booklet_types['law'])

    def get_last_takana(self):
        return self.get_last_of_type(self.booklet_types['takana'])

    def get_takana(self, booklet_number):
        print(booklet_number, type(booklet_number))
        with self.conn:
            return self.conn.execute(f'''SELECT id, file_name, booklet_number, booklet_creation_date
            FROM booklet
            WHERE booklet_type = {self.booklet_types['takana']} AND booklet_number = :booklet_number''',
                                     {'booklet_number': booklet_number}).fetchone()
