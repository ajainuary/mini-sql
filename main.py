import csv
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

if __name__ == "__main__":
    init()
    