import pandas as pd
import sqlite3
import argparse
import signal
TIMEOUT = 10

class DataProcess:
    def __init__(self, filename, crsr):
        self.name = filename[:-4]
        self.crsr = crsr
        try:
            exist = self.crsr.execute("SELECT * FROM table1")
        except Exception as e:
            exist = None

        if exist is None:
            self.df = pd.read_csv(filename)

            self.df.columns = [col.replace(' ', '_').lower() for col in self.df.columns]
            self.df.columns = [col.replace(':', '_').lower() for col in self.df.columns]
            self.df.columns = [col.replace('.', '_').lower() for col in self.df.columns]

            qmark = "?"
            columns = list(self.df.columns)
            head = list(self.df.loc[0])

            q_sql1 = "CREATE TABLE table1 ( "
            for i in range(len(columns)):
                q_sql1 = q_sql1 + columns[i] + " " + self.typedetection(head[i]) + ", "

            q_sql1 = q_sql1[:-2] + ");"

            self.crsr.execute(q_sql1)

            for cols in range(1, len(columns)):
                qmark += ",?"

            for i in range(0, len(self.df)):
                row_data = list(self.df.loc[i])

                self.crsr.execute('''INSERT INTO table1 VALUES ({q})'''.format(q=qmark), row_data)

        else:
            pass



    def query(self):
        print('\n[.] Query Mode Activated (Press Ctrl-C to Quit) [.]\n')
        try:
            while True:
                qry = input(">>")
                self.crsr.execute(qry)
                rows = self.crsr.fetchall()
                for row in rows:
                    print(row, end=', ')
                    pass
        except KeyboardInterrupt:
            pass

    @staticmethod
    def typedetection(val):
        try:
            float(val)
            return "float"
        except ValueError:
            return "varchar(255)"


class TimeoutExpired(Exception):
    pass

def interrupted(signum, frame):
    """called after time out"""
    print('[!] Using default database name')
    raise TimeoutExpired

def input_value():
    try:
        print("[*] Type a database name ... (in 10 sec) Otherwise use default as 'mydb.db'")
        name = input()
        return name
    except TimeoutExpired:
        return "mydb.db"

if __name__=="__main__":
    parser = argparse.ArgumentParser(description='')
    parser.add_argument("-f", "--file", help="filename")

    args = parser.parse_args()

    signal.signal(signal.SIGALRM, interrupted)
    signal.alarm(TIMEOUT)
    name = input_value()
    signal.alarm(0)

    connection = sqlite3.connect(name)
    crsr = connection.cursor()

    dp = DataProcess(args.file, crsr)
    dp.query()
    
    connection.commit()
    connection.close()