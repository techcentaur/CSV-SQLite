import pandas as pd
import sqlite3
import argparse


class DataProcess:
    def __init__(self, filename, crsr):
        self.df = pd.read_csv(filename)
        self.name = filename[:-4]
        crsr = crsr

        self.df.columns = [col.replace(' ', '_').lower() for col in self.df.columns]
        self.df.columns = [col.replace(':', '_').lower() for col in self.df.columns]
        self.df.columns = [col.replace('.', '_').lower() for col in self.df.columns]

        columns = list(self.df.columns)
        head = list(self.df.loc[0])

        q_sql1 = "CREATE TABLE table1 ( "
        for i in range(len(columns)):
            q_sql1 = q_sql1 + columns[i] + " " + self.typedetection(head[i]) + ", "

        q_sql1 = q_sql1[:-2] + ");"

        crsr.execute(q_sql1)

    @staticmethod
    def typedetection(val):
        try:
            float(val)
            return "float"
        except ValueError:
            return "varchar(255)"


if __name__=="__main__":
    parser = argparse.ArgumentParser(description='')
    parser.add_argument("-f", "--file", help="filename")

    args = parser.parse_args()

    connection = sqlite3.connect("myDB.db")
    crsr = connection.cursor()

    dp = DataProcess(args.file, crsr)

    connection.commit()
    connection.close()