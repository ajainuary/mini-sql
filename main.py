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
        while not statement.tokens[i].match(sqlparse.tokens.DML, "SELECT"):
            i = i + 1
        i = i + 1
        while not statement.tokens[i].match(sqlparse.tokens.Keyword, "FROM"):
            if not statement.tokens[i].match(sqlparse.tokens.Whitespace, [" ", "    "]):
                if isinstance(statement.tokens[i], sqlparse.sql.IdentifierList):
                    for identifier in statement.tokens[i].get_identifiers():
                        attributeList.append(identifier)
                else:
                    attributeList.append(statement.tokens[i])
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
        value = None
        for data in row.values():
            for colName in data:
                if colName == location[0] and value is None:
                    value = int(data[colName])
                elif colName == location[0]:
                    raise Exception('Ambiguous column name: {}'.format(location[0]))
        if value is None:
            raise Exception('Could not find Identifier: {}'.format(identifier.value))
        return value
    elif len(location) == 2:
        if not location[0] in row:
            raise Exception('No table with name {} exists'.format(location[0]))
        for colName in row[location[0]]:
            if colName == location[1]:
                return int(row[location[0]][colName])
    raise Exception('Could not find Identifier: {}'.format(identifier.value))

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
    if len(checks) == 1:
        return checks[0]
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

def selectAttributes(row, attributeList, notPrint):
    outputRow = []
    for attr in attributeList:
        if attr.ttype == sqlparse.tokens.Wildcard:
            for table in row:
                for col in row[table]:
                    if [table, col] not in notPrint:
                        outputRow.append(row[table][col])
            return outputRow
        elif isinstance(attr, sqlparse.sql.Function):
            try:
                outputRow.append(extractValue(row, attr[1][1]))
            except:
                raise Exception('Incorrect function usage: {}'.format(attr.value))
        else:
            outputRow.append(extractValue(row, attr))
    return outputRow

def aggregateAttributes(output, attributeList):
    aggregrate = False
    for attr in attributeList:
        if isinstance(attr, sqlparse.sql.Function):
            aggregrate = True
    for attr in attributeList:
        if aggregrate != isinstance(attr, sqlparse.sql.Function):
            raise Exception('Cannot use aggregrate functions with regular attributes')
    if not aggregrate:
        return output
    for i in range(1, len(output)):
        for j in range(len(attributeList)):
            if attributeList[j][0].value.upper() == 'MAX':
                output[0][j] = max(output[i][j], output[0][j])
            elif attributeList[j][0].value.upper() == 'MIN':
                output[0][j] = min(output[i][j], output[0][j])
            elif attributeList[j][0].value.upper() == 'SUM':
                output[0][j] = output[i][j] + output[0][j]
            elif attributeList[j][0].value.upper() == 'AVERAGE':
                output[0][j] = (i*output[0][j] + output[i][j])/(i+1)
    return [output[0]]

def execute(attributeList, tableList, condition, notPrint):
    csvFiles = [open(table+'.csv', newline='') for table in tableList]
    readers = [csv.DictReader(csvFiles[i], DB[tableList[i]]) for i in range(len(tableList))]
    currentTuple = {tableList[i]: next(readers[i]) for i in range(len(tableList)-1)}
    loop = True
    idx = len(tableList)-1
    output = []
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
                output.append(selectAttributes(currentTuple, attributeList, notPrint))
    for csvfile in csvFiles:
        csvfile.close()
    return output

def printHeader(attributeList, tableList, notPrint):
    flag = False
    for attr in attributeList:
        if flag:
            print(", ", end='')
        else:
            print('<', end='')
            flag = True
        if isinstance(attr, sqlparse.sql.Identifier):
            location = []
            for name in attr:
                if name.ttype != sqlparse.tokens.Punctuation:
                    location.append(name.value)
            if len(location) == 1:
                for table in tableList:
                    fl = False
                    for col in DB[table]:
                        if col == location[0]:
                            print(table, col, sep='.', end='')
                            fl = True
                            break
                    if fl:
                        break
            elif len(location) == 2:
                if location not in notPrint:
                    print('.'.join(location), end='')
        elif isinstance(attr, sqlparse.sql.Function):
            location = []
            print(attr[0].value, end='(')
            for name in attr[1][1]:
                if name.ttype != sqlparse.tokens.Punctuation:
                    location.append(name.value)
            if len(location) == 1:
                for table in tableList:
                    fl = False
                    for col in DB[table]:
                        if col == location[0]:
                            if [table, col] not in notPrint:
                                print(table, col, sep='.', end=')')
                                fl = True
                                break
                    if fl:
                        break
            elif len(location) == 2:
                print('.'.join(location), end=')')
        elif attr.ttype == sqlparse.tokens.Wildcard:
            flag = False
            for table in tableList:
                for col in DB[table]:
                    if [table, col] not in notPrint:
                        if flag:
                            print(", ", end='')
                        else:
                            flag = True
                        print(table, col, sep='.', end='')
        else:
            print(attr.value, end='')
    print('>')

def distinct(output):
    distinctOutput = []
    duplicate = {}
    for row in output:
        if not (row[0] in duplicate):
            distinctOutput.append(row)
            duplicate[row[0]] = True
    return distinctOutput

def formattedPrint(output):
    for row in output:
        print(", ".join(str(x) for x in row))

if __name__ == "__main__":
    init()
    try:
        attributeList, tableList, condition = parse(sys.argv[1])
    except:
        print('Error parsing the SQL Query')
        exit(1)
    notPrint = []
    for cond in condition:
        if isinstance(cond, sqlparse.sql.Comparison):
            sides = []
            comparator = ''
            for tok in cond:
                if isinstance(tok, sqlparse.sql.Identifier):
                    location = []
                    for t in tok:
                        if t.ttype == sqlparse.tokens.Name:
                            location.append(t.value)
                    sides.append(location)
                elif tok.ttype == sqlparse.tokens.Comparison:
                    comparator = tok.value
            if comparator == '=' and sides[0][1] == sides[1][1]:
                notPrint.append(sides[1])
    try:
        output = execute(attributeList, tableList, condition, notPrint)
        if isinstance(attributeList[0], sqlparse.sql.Function) and attributeList[0][0].value.upper() == 'DISTINCT':
            output = distinct(output)
        else:
            output = aggregateAttributes(output, attributeList)
    except Exception as error:
        print(error)
        exit(1)
    printHeader(attributeList, tableList, notPrint)
    formattedPrint(output)
