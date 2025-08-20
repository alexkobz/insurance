from rudata.DocsAPI import LIMIT

def pages_payloads(pageSize):
    for pageNum in range(1, 10_000, LIMIT):
        yield [{'pageNum': pageNum + i, 'pageSize': pageSize} for i in range(LIMIT)]
