import psycopg2


def key_clauses(query):
    key_word_1 = "select"
    key_word_2 = "from"
    key_word_3 = "where"
    start_index = query.index(key_word_1) + len(key_word_1) + 1
    end_index = query.index(key_word_2, start_index)
    select_clause = query[start_index:end_index - 1]
    select_clause = select_clause.replace(" ", "")
    start_index = query.index(key_word_2) + len(key_word_2) + 1
    end_index = query.index(key_word_3, start_index)
    from_clause = query[start_index:end_index - 1]
    from_clause = from_clause.replace(" ", "")
    return [select_clause, from_clause]


def table_names(from_clause):
    tables = []
    for table in from_clause.split(','):
        tables.append(table.replace(" ", ""))
    return tables


def feature_extraction(tables):
    tables_attributes = {}
    db = "postgres"
    conn = psycopg2.connect("dbname=%s user=postgres password=sdsd" % db)
    cur = conn.cursor()
    for table in tables:
        cur.execute("select column_name from information_schema.columns where table_name='%s'" % table)
        features = cur.fetchall()
        str_attributes = ""
        for feature in features:
            str_attributes += feature[0] + " "
        tables_attributes[table] = str_attributes
    cur.close()
    conn.commit()
    return tables_attributes


def whole_table(query, table_attributes, from_clause):
    whole_table = []
    db = "postgres"
    conn = psycopg2.connect("dbname=%s user=postgres password=sdsd" % db)
    cur = conn.cursor()
    join_features = []
    for table in from_clause.split(','):
        features = table_attributes[table].split(" ")
        for i in range(0, len(features) - 1):
            if features[i] != '':
                join_features.append(features[i])

    key_word_1 = "select"
    key_word_2 = "from"
    start_index = query.index(key_word_1)
    end_index = query.index(key_word_2, start_index)
    select_clause = query[start_index:end_index - 1]
    query = query.replace(select_clause, "select *")
    cur.execute(query)
    whole_table.append(join_features)
    whole_table.append(cur.fetchall())
    cur.close()
    conn.commit()
    return whole_table


def comparison(select_clause, from_clause, query, wholetable):
    db = "postgres"
    conn = psycopg2.connect("dbname=%s user=postgres password=sdsd" % db)
    cur = conn.cursor()
    cur.execute(query)
    requested_table = []
    for ITEM in cur.fetchall():
        seen = 0
        for ITEM2 in requested_table:
            if ITEM[0] == ITEM2[0]:
                seen = 1
                break
        if (seen == 0):
            requested_table.append(ITEM)
    feature_index = []
    for item in select_clause.split(','):
        if '.' in item:
            relation = item[0:item.index('.')]
            feature = item[item.index('.') + 1:]
            num = 0
            for element in from_clause.split(','):
                if (element != relation):
                    num += 1
                else:
                    break
            i = 0
            if num > 0:
                while num >= 0:
                    while (wholetable[0][i] != feature):
                        i += 1
                    i += 1
                    num -= 1
                feature_index.append(i - 1)
            else:
                i = 0
                while (wholetable[0][i] != feature):
                    i += 1
                feature_index.append(i)
        else:
            feature_index.append(wholetable[0].index(item))

    annotate_index = []
    for i in range(0, len(wholetable[0])):
        if "ann" in wholetable[0][i]:
            annotate_index.append(i)

    annotations = []
    for row in requested_table:
        a = []
        for r in wholetable[1]:
            item = []
            for index in feature_index:
                item.append(r[index])
            if tuple(item) == row:
                for i in annotate_index:
                    a.append(r[i])
                annotations.append(a)
                break
    return annotations


def output_production(query, annotations):
    db = "postgres"
    conn = psycopg2.connect("dbname=%s user=postgres password=sdsd" % db)
    cur = conn.cursor()
    cur.execute(query)
    rows = []
    for ITEM in cur.fetchall():
        seen = 0
        for ITEM2 in rows:
            if ITEM[0] == ITEM2[0]:
                seen = 1
                break
        if (seen == 0):
            rows.append(ITEM)
    for i in range(0, len(rows)):
        row = list(rows[i])
        row.append(",".join(annotations[i]))
        print(row)


def main():
    query = input("Key in your query: ")
    [select_clause, from_clause] = key_clauses(query)
    tables_attributes = feature_extraction(table_names(from_clause))
    wholetable = whole_table(query, tables_attributes, from_clause)
    annotations = comparison(select_clause, from_clause, query, wholetable)
    output_production(query, annotations)


main()
