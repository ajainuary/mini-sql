import csv
import sys
import sqlparse

DB = {}
def init():
    """Initializes the DB structure from metadata.txt"""
    with open('metadata.txt', newline='') as metadata:
        currentTable = None
        for line in metadata:
            text = line.rstrip()
            if text == "<begin_table>" or text == "<end_table>":
                currentTable = None
            elif currentTable == None:
                currentTable = text
                DB[currentTable] = []
            else:
                DB[currentTable].append(text)
def execute(query):
    """Parses and executes the SQL Query
    Args:
        query (str): SQL query string
    """
    parsedQuery = sqlparse.parse(query)
    for statement in parsedQuery:
        attributeList = []
        tableList = []
        condition = []
        length = len(statement.tokens)
        i = 0
        for token in statement.tokens:
            print("DBG:", token.value, type(token.ttype))
        #TODO: Error handling for invalid syntax
        while not statement.tokens[i].match(sqlparse.tokens.DML, "SELECT"):
            i = i + 1
        i = i + 1
        while not statement.tokens[i].match(sqlparse.tokens.Keyword, "FROM"):
            if not statement.tokens[i].match(sqlparse.tokens.Whitespace, [" ", "    "]):
                attributeList.append(statement.tokens[i].value)
            i = i + 1
        i = i + 1
        while i < length and (not statement.tokens[i].match(sqlparse.tokens.Keyword, "WHERE")):
            if not (statement.tokens[i].match(sqlparse.tokens.Whitespace, [" ", "    "]) or statement.tokens[i].match(sqlparse.tokens.Punctuation, ';')):
                tableList.append(statement.tokens[i].value)
            i = i + 1
        print("Attribute List:", attributeList)
        print("Table List:", tableList)

if __name__ == "__main__":
    init()
    execute(sys.argv[1])