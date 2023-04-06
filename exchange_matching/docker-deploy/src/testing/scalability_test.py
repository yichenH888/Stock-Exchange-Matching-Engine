import socket
import xml.etree.ElementTree as ET
import time

class TradingSystem:
    def __init__(self, host, port, buffer_size):
        self.host = host
        self.port = port
        self.buffer_size = buffer_size

    def generate_create_xml(self, id, balance, position):
        root = ET.Element("create")
        ET.SubElement(root, "account", id=id, balance=balance)
        for sym, amount in position.items():
            info = ET.SubElement(root, "symbol", sym=sym)
            ET.SubElement(info, "account", id=id).text = amount
        tree = ET.tostring(root)
        tree = str(len(tree)) + "\n" + tree.decode("utf8", "strict")   
        return tree.encode("utf8", "strict")

    def generate_trans_xml(self, id, order, query, cancel):
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

    def create_socket(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((self.host, self.port))
        return s

    def generate_create_request(self, id, balance, position):
        s = self.create_socket()
        s.sendall(self.generate_create_xml(id, balance, position))


    def generate_transaction_request(self, id, order, query=None, cancel=None):
        s = self.create_socket()
        a = self.generate_trans_xml(id, order, query, cancel)
        s.sendall(a)

def test(trading_system):
    trading_system.generate_create_request("1", "100000", {"AAPL": "100", "GOOG": "200"})
    trading_system.generate_create_request("2", "100000", {"AAPL": "200", "GOOG": "100"})
    trading_system.generate_transaction_request("1", [("AAPL",50,100)])
    trading_system.generate_transaction_request("2", [("AAPL",-50,100)])
    trading_system.generate_transaction_request("1", None, ["1"])
    trading_system.generate_transaction_request("2", None, ["0"])
    trading_system.generate_transaction_request("1", None, None, ["0"])
    trading_system.generate_transaction_request("2", None, None, ["1"])
    trading_system.generate_create_request("3", "100000", {"AMZ": "100", "NFX": "200"})
    trading_system.generate_create_request("4", "100000", {"AMZ": "200", "NFX": "100"})
    trading_system.generate_transaction_request("3", [("AMZ",50,100)])
    trading_system.generate_transaction_request("4", [("AMZ",-40,100)])
    trading_system.generate_transaction_request("3", None, ["2"])
    trading_system.generate_transaction_request("4", None, ["3"])
    trading_system.generate_transaction_request("3", None, None, ["2"])
    trading_system.generate_transaction_request("4", None, None, ["3"])
    trading_system.generate_create_request("5", "100000", {"AAA": "100", "BBB": "200"})
    trading_system.generate_create_request("6", "100000", {"AAA": "200", "BBB": "100"})
    trading_system.generate_transaction_request("5", [("AAA",50,100)])
    trading_system.generate_transaction_request("6", [("AAA",-50,30)])
    trading_system.generate_transaction_request("5", None, ["4"])
    trading_system.generate_transaction_request("6", None, ["5"])
    trading_system.generate_transaction_request("5", None, None, ["4"])
    trading_system.generate_transaction_request("6", None, None, ["5"])
    trading_system.generate_create_request("7", "100000", {"CCC": "100", "DDD": "200"})
    trading_system.generate_create_request("8", "100000", {"CCC": "200", "DDD": "100"})
    trading_system.generate_transaction_request("7", [("CCC",50,100)])
    trading_system.generate_transaction_request("8", [("CCC",-30,20)])
    trading_system.generate_transaction_request("7", None, ["6"])
    trading_system.generate_transaction_request("8", None, ["7"])
    trading_system.generate_transaction_request("7", None, None, ["6"])
    trading_system.generate_transaction_request("8", None, None, ["7"])
    trading_system.generate_create_request("9", "100000", {"EEE": "100", "FFF": "200"})
    trading_system.generate_create_request("10", "100000", {"EEE": "200", "FFF": "100"})
    trading_system.generate_transaction_request("9", [("EEE",50,100)])
    trading_system.generate_transaction_request("10", [("EEE",-60,20)])
    trading_system.generate_transaction_request("9", None, ["8"])
    trading_system.generate_transaction_request("10", None, ["9"])
    trading_system.generate_transaction_request("9", None, None, ["8"])
    trading_system.generate_transaction_request("10", None, None, ["9"])
    trading_system.generate_create_request("11", "100000", {"GGG": "100", "HHH": "200"})
    trading_system.generate_create_request("12", "100000", {"GGG": "200", "HHH": "100"})
    trading_system.generate_transaction_request("11", [("GGG",-30,30)])
    trading_system.generate_transaction_request("12", [("GGG",60,50)])
    trading_system.generate_transaction_request("11", None, ["10"])
    trading_system.generate_transaction_request("12", None, ["11"])
    trading_system.generate_transaction_request("11", None, None, ["10"])
    trading_system.generate_transaction_request("12", None, None, ["11"])
    trading_system.generate_create_request("13", "100000", {"III": "100", "JJJ": "200"})
    trading_system.generate_create_request("14", "100000", {"III": "200", "JJJ": "100"})
    trading_system.generate_transaction_request("13", [("III",-60,30)])
    trading_system.generate_transaction_request("14", [("III",30,50)])
    trading_system.generate_transaction_request("13", None, ["12"])
    trading_system.generate_transaction_request("14", None, ["13"])
    trading_system.generate_transaction_request("13", None, None, ["12"])
    trading_system.generate_transaction_request("14", None, None, ["13"])
    trading_system.generate_create_request("15", "100000", {"KKK": "100", "LLL": "200"})
    trading_system.generate_create_request("16", "100000", {"KKK": "200", "LLL": "100"})
    trading_system.generate_transaction_request("15", [("KKK",-60,100)])
    trading_system.generate_transaction_request("16", [("KKK",30,20)])
    trading_system.generate_transaction_request("15", None, ["14"])
    trading_system.generate_transaction_request("16", None, ["15"])
    trading_system.generate_transaction_request("15", None, None, ["14"])
    trading_system.generate_transaction_request("16", None, None, ["15"])
    trading_system.generate_create_request("17", "100000", {"MMM": "100", "NNN": "200"})
    trading_system.generate_create_request("18", "100000", {"MMM": "200", "NNN": "100"})
    trading_system.generate_transaction_request("17", [("MMM",50,20)])
    trading_system.generate_transaction_request("18", [("MMM",-60,50)])
    trading_system.generate_transaction_request("17", None, ["16"])
    trading_system.generate_transaction_request("18", None, ["17"])
    trading_system.generate_transaction_request("17", None, None, ["16"])
    trading_system.generate_transaction_request("18", None, None, ["17"])
    trading_system.generate_create_request("19", "100000", {"OOO": "100", "PPP": "200"})
    trading_system.generate_create_request("20", "100000", {"OOO": "200", "PPP": "100"})
    trading_system.generate_create_request("21", "100000", {"OOO": "200", "PPP": "100"})
    trading_system.generate_transaction_request("19", [("OOO",-50,30)])
    trading_system.generate_transaction_request("20", [("OOO",-50,20)])
    trading_system.generate_transaction_request("21", [("OOO",100,50)])
    trading_system.generate_transaction_request("19", None, ["18"])
    trading_system.generate_transaction_request("20", None, ["19"])
    trading_system.generate_transaction_request("21", None, ["20"])
    trading_system.generate_create_request("22", "100000", {"QQQ": "100", "RRR": "200"})
    trading_system.generate_create_request("23", "100000", {"QQQ": "200", "RRR": "100"})
    trading_system.generate_create_request("24", "100000", {"QQQ": "200", "RRR": "100"})
    trading_system.generate_transaction_request("22", [("QQQ",-50,30)])
    trading_system.generate_transaction_request("23", [("QQQ",-50,20)])
    trading_system.generate_transaction_request("24", [("QQQ",80,50)])
    trading_system.generate_transaction_request("22", None, ["21"])
    trading_system.generate_transaction_request("23", None, ["22"])
    trading_system.generate_transaction_request("24", None, ["23"])
    trading_system.generate_create_request("25", "100000", {"SSS": "100", "TTT": "200"})
    trading_system.generate_create_request("26", "100000", {"SSS": "200", "TTT": "100"})
    trading_system.generate_create_request("27", "100000", {"SSS": "200", "TTT": "100"})
    trading_system.generate_transaction_request("25", [("SSS",50,30)])
    trading_system.generate_transaction_request("26", [("SSS",50,20)])
    trading_system.generate_transaction_request("27", [("SSS",-100,10)])
    trading_system.generate_transaction_request("25", None, ["24"])
    trading_system.generate_transaction_request("26", None, ["25"])
    trading_system.generate_transaction_request("27", None, ["26"])
    trading_system.generate_create_request("28", "100000", {"UUU": "100", "VVV": "200"})
    trading_system.generate_create_request("29", "100000", {"UUU": "200", "VVV": "100"})
    trading_system.generate_create_request("30", "100000", {"UUU": "200", "VVV": "100"})
    trading_system.generate_transaction_request("28", [("UUU",50,30)])
    trading_system.generate_transaction_request("29", [("UUU",50,20)])
    trading_system.generate_transaction_request("30", [("UUU",-80,10)])
    trading_system.generate_transaction_request("28", None, ["27"])
    trading_system.generate_transaction_request("29", None, ["28"])
    trading_system.generate_transaction_request("30", None, ["29"])


if __name__ == "__main__":
    trading_system = TradingSystem("vcm-30697.vm.duke.edu", 12345, 99999)

    count_num = 0
    start_time = (int)(time.time() * 1000)
    while count_num <= 1:
        test(trading_system)
        count_num = count_num + 1
    end_time = (int)(time.time() * 1000)
    print("Time cost: " + str(end_time - start_time))