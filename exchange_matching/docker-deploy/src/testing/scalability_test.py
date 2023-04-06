import socket
import xml.etree.ElementTree as ET
import time

Host = "vcm-30697.vm.duke.edu"
# HOST = "localhost"
Port = 12345
Buffer = 99999
# Create a TCP/IP socket

def generateCreateXML(id, balance, position):
    root = ET.Element("create")
    ET.SubElement(root, "account", id=id, balance=balance)
    for sym, amount in position.items():
        info = ET.SubElement(root, "symbol", sym = sym)
        ET.SubElement(info, "account", id=id).text = amount
    tree = ET.tostring(root)
    tree = str(len(tree)) + "\n" + tree.decode("utf8", "strict")   
    return tree.encode("utf8", "strict")

def generateTransXML( id, order, query , cancel):
    root = ET.Element("transactions", id=id)
    if order:
        for symbol, amount, limit in order:
            ET.SubElement(root, "order", sym=symbol, amount=str(amount), limit=str(limit))
    if query:
        for transid in query:
            ET.SubElement(root, "query", id=transid)
    if cancel:
        for transid in cancel:
            ET.SubElement(root, "cancel", id=transid)

    tree = ET.tostring(root)
    tree = str(len(tree)) + "\n" + tree.decode("utf8", "strict")
    return tree.encode("utf8", "strict")

def generateCreateRequest(id, balance, position):
    # create an INET, STREAMing socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((Host, Port))
    #generate xml string
    # print(generateCreateXML(id, balance, position))
    s.sendall(generateCreateXML(id, balance, position))

    #receive response
    try:
        data = s.recv(Buffer)
        s.close()
        print (data)
        print("=============================")
        
    except Exception as e:
        print(e)
        print("Exception raised.")
        s.close()
        return

def generateTransactionRequest(id, order, query = None, cancel = None):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((Host, Port))
    #generate xml string
    # print(generateTransXML(id, order, query, cancel))
    a = generateTransXML(id, order, query, cancel)
    s.sendall(a)

    #receive response
    try:
        data = s.recv(Buffer)
        s.close()
        print (data)
        print("=============================")
        
    except Exception as e:
        print(e)
        print("Exception raised.")
        s.close()
        return

# match
def test1():
    generateCreateRequest("1", "100000", {"AAPL": "100", "GOOG": "200"})
    generateCreateRequest("2", "100000", {"AAPL": "200", "GOOG": "100"})
    generateTransactionRequest("1", [("AAPL",50,100)])
    generateTransactionRequest("2", [("AAPL",-50,100)])
    generateTransactionRequest("1", None, ["1"])
    generateTransactionRequest("2", None, ["0"])
    generateTransactionRequest("1", None, None, ["0"])
    generateTransactionRequest("2", None, None, ["1"])

# match
def test2():
    generateCreateRequest("3", "100000", {"AMZ": "100", "NFX": "200"})
    generateCreateRequest("4", "100000", {"AMZ": "200", "NFX": "100"})
    generateTransactionRequest("3", [("AMZ",50,100)])
    generateTransactionRequest("4", [("AMZ",-40,100)])
    generateTransactionRequest("3", None, ["2"])
    generateTransactionRequest("4", None, ["3"])
    generateTransactionRequest("3", None, None, ["2"])
    generateTransactionRequest("4", None, None, ["3"])

# match
def test3():
    generateCreateRequest("5", "100000", {"AAA": "100", "BBB": "200"})
    generateCreateRequest("6", "100000", {"AAA": "200", "BBB": "100"})
    generateTransactionRequest("5", [("AAA",50,100)])
    generateTransactionRequest("6", [("AAA",-50,30)])
    generateTransactionRequest("5", None, ["4"])
    generateTransactionRequest("6", None, ["5"])
    generateTransactionRequest("5", None, None, ["4"])
    generateTransactionRequest("6", None, None, ["5"])

# match
def test4():
    generateCreateRequest("7", "100000", {"CCC": "100", "DDD": "200"})
    generateCreateRequest("8", "100000", {"CCC": "200", "DDD": "100"})
    generateTransactionRequest("7", [("CCC",50,100)])
    generateTransactionRequest("8", [("CCC",-30,20)])
    generateTransactionRequest("7", None, ["6"])
    generateTransactionRequest("8", None, ["7"])
    generateTransactionRequest("7", None, None, ["6"])
    generateTransactionRequest("8", None, None, ["7"])

# match
def test5():
    generateCreateRequest("9", "100000", {"EEE": "100", "FFF": "200"})
    generateCreateRequest("10", "100000", {"EEE": "200", "FFF": "100"})
    generateTransactionRequest("9", [("EEE",50,100)])
    generateTransactionRequest("10", [("EEE",-60,20)])
    generateTransactionRequest("9", None, ["8"])
    generateTransactionRequest("10", None, ["9"])
    generateTransactionRequest("9", None, None, ["8"])
    generateTransactionRequest("10", None, None, ["9"])

# match
def test6():
    generateCreateRequest("11", "100000", {"GGG": "100", "HHH": "200"})
    generateCreateRequest("12", "100000", {"GGG": "200", "HHH": "100"})
    generateTransactionRequest("11", [("GGG",-30,30)])
    generateTransactionRequest("12", [("GGG",60,50)])
    generateTransactionRequest("11", None, ["10"])
    generateTransactionRequest("12", None, ["11"])
    generateTransactionRequest("11", None, None, ["10"])
    generateTransactionRequest("12", None, None, ["11"])

# match
def test7():
    generateCreateRequest("13", "100000", {"III": "100", "JJJ": "200"})
    generateCreateRequest("14", "100000", {"III": "200", "JJJ": "100"})
    generateTransactionRequest("13", [("III",-60,30)])
    generateTransactionRequest("14", [("III",30,50)])
    generateTransactionRequest("13", None, ["12"])
    generateTransactionRequest("14", None, ["13"])
    generateTransactionRequest("13", None, None, ["12"])
    generateTransactionRequest("14", None, None, ["13"])

# no match
def test8():
    generateCreateRequest("15", "100000", {"KKK": "100", "LLL": "200"})
    generateCreateRequest("16", "100000", {"KKK": "200", "LLL": "100"})
    generateTransactionRequest("15", [("KKK",-60,100)])
    generateTransactionRequest("16", [("KKK",30,20)])
    generateTransactionRequest("15", None, ["14"])
    generateTransactionRequest("16", None, ["15"])
    generateTransactionRequest("15", None, None, ["14"])
    generateTransactionRequest("16", None, None, ["15"])

# no match
def test9():
    generateCreateRequest("17", "100000", {"MMM": "100", "NNN": "200"})
    generateCreateRequest("18", "100000", {"MMM": "200", "NNN": "100"})
    generateTransactionRequest("17", [("MMM",50,20)])
    generateTransactionRequest("18", [("MMM",-60,50)])
    generateTransactionRequest("17", None, ["16"])
    generateTransactionRequest("18", None, ["17"])
    generateTransactionRequest("17", None, None, ["16"])
    generateTransactionRequest("18", None, None, ["17"])

# multiple match
def test10():
    generateCreateRequest("19", "100000", {"OOO": "100", "PPP": "200"})
    generateCreateRequest("20", "100000", {"OOO": "200", "PPP": "100"})
    generateCreateRequest("21", "100000", {"OOO": "200", "PPP": "100"})
    generateTransactionRequest("19", [("OOO",-50,30)])
    generateTransactionRequest("20", [("OOO",-50,20)])
    generateTransactionRequest("21", [("OOO",100,50)])
    generateTransactionRequest("19", None, ["18"])
    generateTransactionRequest("20", None, ["19"])
    generateTransactionRequest("21", None, ["20"])
    # generateTransactionRequest("19", None, None, ["18"])
    # generateTransactionRequest("20", None, None, ["19"])
    # generateTransactionRequest("21", None, None, ["20"])

# multiple match
def test11():
    generateCreateRequest("22", "100000", {"QQQ": "100", "RRR": "200"})
    generateCreateRequest("23", "100000", {"QQQ": "200", "RRR": "100"})
    generateCreateRequest("24", "100000", {"QQQ": "200", "RRR": "100"})
    generateTransactionRequest("22", [("QQQ",-50,30)])
    generateTransactionRequest("23", [("QQQ",-50,20)])
    generateTransactionRequest("24", [("QQQ",80,50)])
    generateTransactionRequest("22", None, ["21"])
    generateTransactionRequest("23", None, ["22"])
    generateTransactionRequest("24", None, ["23"])
    # generateTransactionRequest("19", None, None, ["18"])
    # generateTransactionRequest("20", None, None, ["19"])
    # generateTransactionRequest("21", None, None, ["20"])

# multiple match
def test12():
    generateCreateRequest("25", "100000", {"SSS": "100", "TTT": "200"})
    generateCreateRequest("26", "100000", {"SSS": "200", "TTT": "100"})
    generateCreateRequest("27", "100000", {"SSS": "200", "TTT": "100"})
    generateTransactionRequest("25", [("SSS",50,30)])
    generateTransactionRequest("26", [("SSS",50,20)])
    generateTransactionRequest("27", [("SSS",-100,10)])
    generateTransactionRequest("25", None, ["24"])
    generateTransactionRequest("26", None, ["25"])
    generateTransactionRequest("27", None, ["26"])
    # generateTransactionRequest("19", None, None, ["18"])
    # generateTransactionRequest("20", None, None, ["19"])
    # generateTransactionRequest("21", None, None, ["20"])

# multiple match
def test13():
    generateCreateRequest("28", "100000", {"UUU": "100", "VVV": "200"})
    generateCreateRequest("29", "100000", {"UUU": "200", "VVV": "100"})
    generateCreateRequest("30", "100000", {"UUU": "200", "VVV": "100"})
    generateTransactionRequest("28", [("UUU",50,30)])
    generateTransactionRequest("29", [("UUU",50,20)])
    generateTransactionRequest("30", [("UUU",-80,10)])
    generateTransactionRequest("28", None, ["27"])
    generateTransactionRequest("29", None, ["28"])
    generateTransactionRequest("30", None, ["29"])
    # generateTransactionRequest("19", None, None, ["18"])
    # generateTransactionRequest("20", None, None, ["19"])
    # generateTransactionRequest("21", None, None, ["20"])



# sell position that user doesn't have
def test14():
    generateCreateRequest("31", "100000", {"WWW": "100", "XXX": "200"})
    generateCreateRequest("32", "100000", {"WWW": "200", "XXX": "100"})
    generateCreateRequest("33", "100000", {"WWW": "200", "XXX": "100"})
    generateTransactionRequest("31", [("WWW",-50,30)])
    generateTransactionRequest("32", [("AAA",-50,20)])
    generateTransactionRequest("33", [("AAA",100,50)])
    generateTransactionRequest("31", None, ["30"])
    generateTransactionRequest("32", None, ["31"])
    generateTransactionRequest("33", None, ["32"])
    # generateTransactionRequest("19", None, None, ["18"])
    # generateTransactionRequest("20", None, None, ["19"])
    # generateTransactionRequest("21", None, None, ["20"])

if __name__ == "__main__":
    count_num = 0
    start_time = (int)(time.time() * 1000)
    while count_num <= 70:
        test1()
        test2()
        test3()
        test4()
        test5()
        test6()
        test7()
        test8()
        test9()
        test10()
        test11()
        test12()
        test13()
        # generateCreateRequest(str(count_num), "100000", {"WWW": "100", "XXX": "200"})
        count_num = count_num + 1
    end_time = (int)(time.time() * 1000)
    print("Time cost: " + str(end_time - start_time))
    
    
    # test14()
    # 14测出来问题: user A 卖自己不拥有的position