from simpledb import Database
from simpledb.result import QueryResult


def test_index_backed_select(tmp_path):
    db = Database.open(tmp_path)
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email VARCHAR(255) UNIQUE NOT NULL);")
    db.execute("INSERT INTO users (id, email) VALUES (1, 'a@b.com');")
    db.execute("INSERT INTO users (id, email) VALUES (2, 'c@d.com');")

    db.execute("CREATE INDEX idx_email ON users(email);")

    res = db.execute("SELECT id FROM users WHERE email = 'c@d.com';")
    assert isinstance(res, QueryResult)
    assert res.rows == [[2]]
    assert res.stats is not None
    assert res.stats["plan"] == "index"
    assert res.stats["index"] == "idx_email"