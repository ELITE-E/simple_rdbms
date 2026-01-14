from simpledb import Database
from simpledb.result import QueryResult


def test_join_transactions_categories(tmp_path):
    db = Database.open(tmp_path)

    db.execute("CREATE TABLE categories (id INTEGER PRIMARY KEY, user_id INTEGER, name VARCHAR(50) UNIQUE);")
    db.execute("CREATE TABLE transactions (id INTEGER PRIMARY KEY, user_id INTEGER, category_id INTEGER, amount INTEGER);")

    db.execute("INSERT INTO categories (id, user_id, name) VALUES (1, 10, 'Groceries');")
    db.execute("INSERT INTO categories (id, user_id, name) VALUES (2, 10, 'Rent');")

    db.execute("INSERT INTO transactions (id, user_id, category_id, amount) VALUES (100, 10, 1, 2500);")
    db.execute("INSERT INTO transactions (id, user_id, category_id, amount) VALUES (101, 10, 2, 50000);")

    # Index makes join faster (index nested-loop)
    db.execute("CREATE INDEX idx_cat_id ON categories(id);")

    res = db.execute(
        "SELECT transactions.id, categories.name "
        "FROM transactions "
        "JOIN categories ON transactions.category_id = categories.id "
        "WHERE transactions.user_id = 10;"
    )
    assert isinstance(res, QueryResult)
    assert res.rows == [
        [100, "Groceries"],
        [101, "Rent"],
    ]
    assert res.stats is not None
    assert res.stats["plan"] == "join"