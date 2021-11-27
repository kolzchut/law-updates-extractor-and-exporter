import sqlite3


class Database:
    def __enter__(self):
        self.conn = sqlite3.connect('kzdb.sqlite')
        self.conn.row_factory = sqlite3.Row
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()

    def insert_takana(self, takana):
        takana_id = 2
        with self.conn:
            self.conn.execute(f'''INSERT INTO booklet 
                (file_name, extension, booklet_number, number_of_pages, description,
                booklet_creation_date, modify_date, published_date, booklet_type,
                display_name, foreign_year)
                VALUES (:file_name, :extension, :booklet_number, :number_of_pages, :description,
                :creation_date, :modify_date, :published_date, {takana_id}, :display_name,
                :foreign_year)''', takana)

    def insert_law(self, law):
        law_id = 1
        with self.conn:
            self.conn.execute(f'''INSERT INTO booklet 
                (file_name, extension, booklet_number, number_of_pages, description,
                booklet_creation_date, modify_date, published_date, booklet_type,
                display_name, foreign_year)
                VALUES (:file_name, :extension, :booklet_number, :number_of_pages, :description,
                :creation_date, :modify_date, :published_date, {law_id}, :display_name,
                :foreign_year)''', law)

    def get_last_law(self):
        with self.conn:
            return self.conn.execute('''SELECT id, file_name, booklet_number, booklet_creation_date
            FROM booklet WHERE booklet_type = 1 ORDER BY id DESC LIMIT 1''').fetchone()

    def get_last_takana(self):
        with self.conn:
            return self.conn.execute('''SELECT id, file_name, booklet_number, booklet_creation_date
            FROM booklet 
            WHERE booklet_type = 2 ORDER BY id DESC LIMIT 1''').fetchone()

    def get_takana(self, booklet_number):
        print(booklet_number, type(booklet_number))
        with self.conn:
            return self.conn.execute('''SELECT id, file_name, booklet_number, booklet_creation_date
            FROM booklet
            WHERE booklet_type = 2 AND booklet_number = :booklet_number''',
                                     {'booklet_number': booklet_number}).fetchone()
