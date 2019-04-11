#!flask/bin/python
from flask import Flask, jsonify, render_template, request
from pyhive import hive
import json


import pandas as pd

app = Flask(__name__)
conn = hive.connect('localhost', username='hive', port=10000)


@app.route('/', methods=['GET'])
def home():
    return render_template('welcome.html')


@app.route('/list_databases', methods=['GET'])
def list_databases():
    df = pd.read_sql("show databases", conn)
    res = df.to_json(orient='records')
    return jsonify(databases=res)


@app.route('/list_tables/<string:db_name>', methods=['GET'])
def list_tables(db_name):
    cur = conn.cursor()
    cur.execute("use %s" % db_name)
    cur.execute("show tables")
    res = cur.fetchall()
    return jsonify(tables=res)


@app.route('/list_column/<string:db_name>/<string:table_name>', methods=['GET'])
def list_col(db_name, table_name):
    print ("QUERY: ","select * from %s.%s limit 2" % (db_name, table_name))
    df = pd.read_sql("select * from %s.%s limit 2" % (db_name, table_name), conn)
    col = list(df.columns.values)
    #print (col)
    return jsonify(column_names=col)


@app.route('/<query_type>/<string:database_name>/<string:table_name>/<int:limit>')
@app.route('/<query_type>/<string:database_name>/<string:table_name>')
def query(query_type, database_name, table_name, limit=50):
    # data = json.loads(request.data)
    # print (data)
    cur = conn.cursor()
    if query_type == "query_all":
        df = pd.read_sql("select * from %s.%s" % (database_name, table_name), conn)
        res = df.to_json(orient='records')
        return jsonify(result=res)
    if query_type == "query_limited":
        df = pd.read_sql("select * from %s.%s limit %d" % (database_name, table_name, limit), conn)
        res = df.to_json(orient='records')
        return jsonify(result=res)
    if query_type == "query":
        if request.data:
            payload = json.loads(request.data)
        else:
            payload = {}
        colname = payload.get('col_name', '*')
        condition = payload.get('condition', '1=1')
        order_by = payload.get('order_by', 'NULL')
        query_str = "select %s from %s.%s where %s order by %s limit %d" % (
            colname, database_name, table_name, condition, order_by, limit)
        df = pd.read_sql(query_str, conn)
        res = df.to_json(orient='records')
        return jsonify(result=res)


if __name__ == '__main__':
    app.run(debug=True, host="localhost", port=8555)
