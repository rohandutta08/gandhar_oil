#!/usr/bin/env python3
import sys
import sqlglot


def format_sql(sql):
    return sqlglot.transpile(sql, pretty=True)


sql = sys.stdin.read()
formatted_sql = format_sql(sql)
print("".join(formatted_sql))
