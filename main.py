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
def parse(query):
    """Parses the SQL Query
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
        #TODO: Error handling for invalid syntax
        #TODO: Try using ttype comparison instead of match
        while not statement.tokens[i].match(sqlparse.tokens.DML, "SELECT"):
            i = i + 1
        i = i + 1
        while not statement.tokens[i].match(sqlparse.tokens.Keyword, "FROM"):
            if not statement.tokens[i].match(sqlparse.tokens.Whitespace, [" ", "    "]):
                attributeList.append(statement.tokens[i].value)
            i = i + 1
        i = i + 1
        while i < length and (not isinstance(statement.tokens[i], sqlparse.sql.Where)):
            if not (statement.tokens[i].match(sqlparse.tokens.Whitespace, [" ", "    "]) or statement.tokens[i].match(sqlparse.tokens.Punctuation, ';')):
                tableList.append(statement.tokens[i].value)
            i = i + 1
        if i < length:
            conditionStatement = statement.tokens[i].tokens
            for tok in conditionStatement:
                if isinstance(tok, sqlparse.sql.Comparison):
                    condition.append(tok)
                elif isinstance(tok, sqlparse.sql.Token) and tok.match(sqlparse.tokens.Keyword, ["AND", "OR"]):
                    condition.append(tok.value.upper())
        return attributeList, tableList, condition

if __name__ == "__main__":
    init()
    attributeList, tableList, condition = parse(sys.argv[1])
    print(attributeList, tableList, condition)
