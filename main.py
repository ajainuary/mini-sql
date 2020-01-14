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
                if isinstance(statement.tokens[i], sqlparse.sql.IdentifierList):
                    for identifier in statement.tokens[i].get_identifiers():
                        attributeList.append(identifier.value)
                else:
                    attributeList.append(statement.tokens[i].value)
            i = i + 1
        i = i + 1
        while i < length and (not isinstance(statement.tokens[i], sqlparse.sql.Where)):
            if not (statement.tokens[i].match(sqlparse.tokens.Whitespace, [" ", "    "]) or statement.tokens[i].match(sqlparse.tokens.Punctuation, ';')):
                if isinstance(statement.tokens[i], sqlparse.sql.IdentifierList):
                    for identifier in statement.tokens[i].get_identifiers():
                        tableList.append(identifier.value)
                else:
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

def extractValue(row, identifier):
    location = []
    for t in identifier:
        if t.ttype == sqlparse.tokens.Name:
            location.append(t.value)
    if len(location) == 1:
        for data in row.values():
            for colName in data:
                if colName == location[0]:
                    return int(data[colName])
    elif len(location) == 2:
        for colName in row[location[0]]:
            if colName == location[1]:
                return int(row[location[0]][colName])
    #TODO: Handle identifier not found

def conditionCheck(row, condition):
    checks = []
    for cond in condition:
        if isinstance(cond, sqlparse.sql.Comparison):
            values = []
            comparator = ''
            for tok in cond:
                if isinstance(tok, sqlparse.sql.Identifier):
                    values.append(extractValue(row, tok))
                elif tok.ttype == sqlparse.tokens.Comparison:
                    comparator = tok.value
                elif tok.ttype == sqlparse.tokens.Number.Integer:
                    values.append(int(tok.value))
            if comparator == '=':
                checks.append(values[0] == values[1])
            elif comparator == '<':
                checks.append(values[0] < values[1])
            elif comparator == '<=':
                checks.append(values[0] <= values[1])
            elif comparator == '>':
                checks.append(values[0] > values[1])
            elif comparator == '>=':
                checks.append(values[0] >= values[1])
    i = 0
    for cond in condition:
        if type(cond) is str:
            if cond.upper() == 'AND' and not (checks[i] and checks[i+1]):
                return False
            elif cond.upper() == 'OR' and not (checks[i] or checks[i+1]):
                return False
            i = i + 1
            if i <= len(checks)-1:
                return True
    return True

def execute(attributeList, tableList, condition):
    csvFiles = [open(table+'.csv', newline='') for table in tableList]
    readers = [csv.DictReader(csvFiles[i], DB[tableList[i]]) for i in range(len(tableList))]
    currentTuple = {tableList[i]: next(readers[i]) for i in range(len(tableList)-1)}
    loop = True
    idx = len(tableList)-1
    while loop:
        try:
            currentTuple[tableList[idx]] = next(readers[idx])
            idx = min(idx+1, len(tableList)-1)
        except StopIteration:
            csvFiles[idx].seek(0)
            currentTuple[tableList[idx]] = next(readers[idx])
            idx = idx - 1
            if idx < 0:
                loop = False
        else:
            if idx == len(tableList)-1 and conditionCheck(currentTuple, condition):
                print(currentTuple)
    for csvfile in csvFiles:
        csvfile.close()

if __name__ == "__main__":
    init()
    attributeList, tableList, condition = parse(sys.argv[1])
    print(attributeList, tableList, condition)
    execute(attributeList, tableList, condition)
