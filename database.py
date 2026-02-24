import sqlite3


class Database:
    booklet_types = {
        "law": 1,
        "takana": 2,
        "notification": 3
    }

    def __enter__(self):
        self.conn = sqlite3.connect('kzdb.sqlite')
        self.conn.row_factory = sqlite3.Row
        self._ensure_jira_key_column()
        return self

    def _ensure_jira_key_column(self):
        cols = {row['name'] for row in self.conn.execute('PRAGMA table_info(booklet)').fetchall()}
        if 'jira_key' not in cols:
            with self.conn:
                self.conn.execute('ALTER TABLE booklet ADD COLUMN jira_key TEXT')

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()

    def insert_item(self, item_type, item):
        with self.conn:
            cursor = self.conn.execute(f'''INSERT INTO booklet
                (file_name, extension, booklet_number, number_of_pages, description,
                booklet_creation_date, modify_date, published_date, booklet_type,
                display_name, foreign_year)
                VALUES (:file_name, :extension, :booklet_number, :number_of_pages, :description,
                :creation_date, :modify_date, :published_date, {item_type}, :display_name,
                :foreign_year)''', item)
            return cursor.lastrowid

    def insert_takana(self, takana):
        return self.insert_item(self.booklet_types['takana'], takana)

    def insert_law(self, law):
        return self.insert_item(self.booklet_types['law'], law)

    def insert_notification(self, notification):
        return self.insert_item(self.booklet_types['notification'], notification)

    def get_last_of_type(self, item_type):
        with self.conn:
            return self.conn.execute(f'''SELECT id, file_name, booklet_number, booklet_creation_date
            FROM booklet WHERE booklet_type = {item_type} ORDER BY id DESC LIMIT 1''').fetchone()

    def get_last_law(self):
        return self.get_last_of_type(self.booklet_types['law'])

    def get_last_takana(self):
        return self.get_last_of_type(self.booklet_types['takana'])

    def get_last_notification(self):
        return self.get_last_of_type(self.booklet_types['notification'])

    def get_type(self, booklet_type, booklet_number):
        return self.conn.execute(f'''SELECT id, file_name, booklet_number, booklet_creation_date
        FROM booklet
        WHERE booklet_type = {booklet_type} AND booklet_number = :booklet_number''',
                                 {'booklet_number': booklet_number}).fetchone()

    def get_law(self, law_number):
        return self.get_type(self.booklet_types['law'], law_number)

    def get_takana(self, takana_number):
        return self.get_type(self.booklet_types['takana'], takana_number)

    def get_notification(self, notification_number):
        return self.get_type(self.booklet_types['notification'], notification_number)

    def update_jira_key(self, booklet_number, booklet_type, jira_key):
        """Record the Jira issue key for a stored booklet. booklet_type may be int or string."""
        if isinstance(booklet_type, str):
            booklet_type = self.booklet_types[booklet_type]
        with self.conn:
            self.conn.execute(
                'UPDATE booklet SET jira_key = :jira_key '
                'WHERE booklet_number = :booklet_number AND booklet_type = :booklet_type',
                {'jira_key': jira_key, 'booklet_number': booklet_number, 'booklet_type': booklet_type}
            )

    def update_jira_key_by_id(self, row_id, jira_key):
        """Record the Jira issue key for a specific row by its primary key."""
        with self.conn:
            self.conn.execute(
                'UPDATE booklet SET jira_key = :jira_key WHERE id = :id',
                {'jira_key': jira_key, 'id': row_id}
            )

    def get_all_without_jira_key(self, from_booklet=None):
        """Return all rows (any type) that have no jira_key yet, ordered by booklet_number.
        Optionally restrict to booklet_number >= from_booklet."""
        if from_booklet is not None:
            rows = self.conn.execute(
                'SELECT * FROM booklet WHERE jira_key IS NULL AND booklet_number >= :from_booklet '
                'ORDER BY booklet_number DESC',
                {'from_booklet': from_booklet}
            ).fetchall()
        else:
            rows = self.conn.execute(
                'SELECT * FROM booklet WHERE jira_key IS NULL ORDER BY booklet_number DESC'
            ).fetchall()
        return [dict(row) for row in rows]

    def get_full_by_booklet_number(self, booklet_number):
        """Return all DB rows (any type) matching booklet_number, as plain dicts."""
        rows = self.conn.execute(
            'SELECT * FROM booklet WHERE booklet_number = :booklet_number',
            {'booklet_number': booklet_number}
        ).fetchall()
        return [dict(row) for row in rows]

    def get_all_entries_of_type(self, item_type):
        """Return a set of (booklet_number, display_name) tuples for all stored rows of this type.
        Used to detect true duplicates: same booklet AND same law title."""
        with self.conn:
            rows = self.conn.execute(
                f'SELECT booklet_number, display_name FROM booklet WHERE booklet_type = {item_type}'
            ).fetchall()
            return {(int(row['booklet_number']), row['display_name']) for row in rows}

    def get_all_law_entries(self):
        return self.get_all_entries_of_type(self.booklet_types['law'])

    def get_all_takana_entries(self):
        return self.get_all_entries_of_type(self.booklet_types['takana'])

    def get_all_notification_entries(self):
        return self.get_all_entries_of_type(self.booklet_types['notification'])

