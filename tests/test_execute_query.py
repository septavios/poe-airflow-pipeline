from unittest.mock import MagicMock, patch
from dashboard.app import execute_query


def create_mock_conn(description=None, rows=None):
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    cursor.description = description
    cursor.fetchall.return_value = rows or []
    return conn, cursor


def test_execute_query_commits_and_returns_rows():
    conn, cursor = create_mock_conn(description=[('col',)], rows=[(1,)])
    with patch('dashboard.app.get_db_connection', return_value=conn):
        result = execute_query('SELECT 1')
    assert result == [{'col': 1}]
    conn.commit.assert_called_once()
    conn.close.assert_called_once()


def test_execute_query_commits_without_result_set():
    conn, cursor = create_mock_conn(description=None)
    with patch('dashboard.app.get_db_connection', return_value=conn):
        result = execute_query('UPDATE foo SET bar=1')
    assert result == []
    conn.commit.assert_called_once()
    conn.close.assert_called_once()
