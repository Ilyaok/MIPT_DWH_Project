import pandas as pd
import jaydebeapi
import logging

from pathlib import Path

log = logging.getLogger(__name__)
here = Path(__file__).absolute().parent

def main():

    conn = jaydebeapi.connect(
        'oracle.jdbc.driver.OracleDriver',
        'jdbc:oracle:thin:demipt2/peregrintook@de-oracle.chronosavant.ru:1521/deoracle',
        ['demipt2', 'peregrintook'],
        'jars/ojdbc8.jar'
    )

    conn.jconn.setAutoCommit(False)
    curs = conn.cursor()

    curs.execute("SELECT * FROM BANK.ACCOUNTS")
    conn.commit()
    result = curs.fetchall()

    names = [ x[0] for x in curs.description ]
    df = pd.DataFrame(result, columns=names)
    print(df)


if __name__ == "__main__":
    main()