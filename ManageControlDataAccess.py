import pandas as pd
import traceback
import click
import logging
import os
import sys
import numpy as np

import snowflake.connector
import snowflake.connector.cursor
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

class ManageControlDataAccess:

    def __init__(self):
        self.account = False
        self.privateKey = None
        self.connType = None
        self.roleName = None
        self.userID = None
        self.con = None
        self.warehouse = None
        self.readOnly = False
        self.roleName = None
        self.warehouse = None
        self.table = None
        self.cdafile = None

    def initializeLogger(self):
        formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(message)s',
                                      datefmt='%Y-%m-%d %H:%M:%S')
        screen_handler = logging.StreamHandler(stream=sys.stdout)
        screen_handler.setFormatter(formatter)
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(screen_handler)

    def initializeConnection(self):
        try:
            if self.connType.lower()=="Basic".lower():
                PASSWORD = os.getenv('SNOWSQL_PWD');
                if PASSWORD is None and self.userID is None:
                    raise Exception("Either USERID or PASSWORD is not provided.")
                self.con = snowflake.connector.connect(
                    user= self.userID,
                    password= PASSWORD,
                    account= self.account,
                    role = self.roleName,
                    warehouse = self.warehouse
                )
            elif self.connType.lower()=="SSO".lower():
                self.con = snowflake.connector.connect(
                    user= self.userID,
                    authenticator='externalbrowser',
                    account= self.account,
                    role = self.roleName,
                    warehouse = self.warehouse
                )
            elif self.connType.lower()=="KEY".lower():
                PASSWORD = os.getenv("PRIVATE_KEY_PASSPHRASE")
                if PASSWORD is None or self.privateKey is None:
                    raise Exception("Either PRIVATE_KEY or PRIVATE_KEY_PASSPHRASE is not provided.")
                with open(self.privateKey, "rb") as key:
                    p_key= serialization.load_pem_private_key(
                        key.read(),
                        password=os.environ['PRIVATE_KEY_PASSPHRASE'].encode(),
                        backend=default_backend()
                    )

                pkb = p_key.private_bytes(
                    encoding=serialization.Encoding.DER,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption())

                self.con = snowflake.connector.connect(
                    user=self.userID,
                    account=self.account,
                    private_key=pkb,
                    role = self.roleName,
                    warehouse = self.warehouse
                )
                click.echo("Connection opened to Snowflake account: %s" %self.account)
        except Exception:
            raise

    def releaseResource(self):
        if self.con is not None:
            click.echo("Connection to snowflake is closed")
            self.con.close()

    def getCDADataFrame(self):
        cur = self.con.cursor()
        try:
            results = cur.execute(f"SELECT * FROM {self.table}")
            df = cur.fetch_pandas_all()
            return df
        except snowflake.connector.ProgrammingError as e:
            self.logger.info(e.msg)
            raise e
        finally:
            cur.close()

    def executeDB(self, statement, data):
        cur = self.con.cursor()
        try:
           cur.execute(statement, data)
        except snowflake.connector.ProgrammingError as e:
            self.logger.info(e.msg)
            raise e
        finally:
            cur.close()

    def containsDuplicate(self, csvDF):
        duplicateRowsDF = csvDF[csvDF.duplicated(subset=['ACCOUNT','ROLE'], keep=False)]
        if(duplicateRowsDF.size > 0):
            self.logger.error("Duplicate rows found in CSV please fix the problem: ")
            self.logger.info(duplicateRowsDF)
            raise Exception("Duplicate rows found in CSV please fix the problem")
        return True

    def processControlDataAccess(self):
        csvDF = pd.read_csv(self.cdafile)
        csvDF["ACCOUNT"]= csvDF['ACCOUNT'].astype(object)
        csvDF["ROLE"]= csvDF['ROLE'].astype(object)

        #Find any duplicate entries in CSV file before proceeding, the process will error if duplicate is found logging the duplicate row.
        self.containsDuplicate(csvDF)
        snDF = self.getCDADataFrame()

        #To be Inserted Into Snowflake
        csvMerge = csvDF.merge(snDF,how="left",on=("ACCOUNT","ROLE"),suffixes=('_csv', '_sf'))
        csvMergeN = csvMerge.where(pd.notnull(csvMerge), None)

        self.logger.info("CSV content participating in insertion, select records who's snowflake warehouse and database entry are NONE: ")
        self.logger.info(csvMergeN)

        for content in csvMergeN.iterrows():
            account     = content[1]["ACCOUNT"]
            role        = content[1]["ROLE"]
            warehouse_csv    = content[1]["WAREHOUSE_csv"]
            database_csv   = content[1]["DATABASE_csv"]
            warehouse_sn    = content[1]["WAREHOUSE_sf"]
            database_sn   = content[1]["DATABASE_sf"]

            if(database_sn is None and warehouse_sn is None and not self.readOnly):
                data = (account, role,warehouse_csv,database_csv)
                statement = "INSERT INTO {0} VALUES (%s, %s, %s, %s )".format(self.table)
                self.executeDB(statement,data)

        #To be Deleted from Snowflake
        snMerge = snDF.merge(csvDF,how="left",on=("ACCOUNT","ROLE"),suffixes=('_sf', '_csv'))
        snMergeN = snMerge.where(pd.notnull(snMerge), None)

        self.logger.info("Snowflake content participating in deletion, select records whos CSV warehouse and database entry are NONE: ")
        self.logger.info(snMergeN)
        for content in snMergeN.iterrows():
            account     = content[1]["ACCOUNT"]
            role        = content[1]["ROLE"]
            warehouse_csv    = content[1]["WAREHOUSE_csv"]
            database_csv   = content[1]["DATABASE_csv"]
            warehouse_sn    = content[1]["WAREHOUSE_sf"]
            database_sn   = content[1]["DATABASE_sf"]

            if(warehouse_csv is None and database_csv is None and not self.readOnly):
                data = []
                if account is not None:
                    data.append(account)
                if role is not None:
                    data.append(role)
                statement = "DELETE FROM {0} WHERE ACCOUNT {1} and ROLE {2}".\
                    format(self.table," IS NULL" if account is None else "=%s", "IS NULL" if role is None else "=%s")
                self.executeDB(statement,data)

        #To be Updated
        sn_csv = csvDF.merge(snDF, on=['ACCOUNT','ROLE'],suffixes=('_csv', '_sf'))
        sn_csv = sn_csv.where(pd.notnull(sn_csv), None)

        self.logger.info("Content participating for update, select records whos snowflake and CSV warehouse and database entry do not match: ")
        self.logger.info(sn_csv)

        for content in sn_csv.iterrows():
            account     = content[1]["ACCOUNT"]
            role        = content[1]["ROLE"]
            warehouse_csv    = content[1]["WAREHOUSE_csv"]
            database_csv   = content[1]["DATABASE_csv"]
            warehouse_sn    = content[1]["WAREHOUSE_sf"]
            database_sn   = content[1]["DATABASE_sf"]

            if(warehouse_csv!=warehouse_sn or database_csv!= database_sn and not self.readOnly):
                data = []
                data.append(warehouse_csv)
                data.append(database_csv)
                if account is not None:
                    data.append(account)
                if role is not None:
                    data.append(role)
                statement = "UPDATE {0} SET WAREHOUSE=%s, DATABASE=%s WHERE ACCOUNT {1} and ROLE {2}".\
                    format(self.table," IS NULL" if account is None else "=%s", "IS NULL" if role is None else "=%s")
                self.executeDB(statement,data)

        if not self.readOnly:
            self.logger.info("Snowflake content participating in execution of stored procedure CDOPS_STATESTORE.REPORTING.SP_VW_SNOWFLAKE_MEMBER_RESOURCE_MAPPING : ")
            statement = "CALL CDOPS_STATESTORE.REPORTING.SP_VW_SNOWFLAKE_MEMBER_RESOURCE_MAPPING()"
            self.executeDB(statement, None)

pass_context = click.make_pass_decorator(ManageControlDataAccess, ensure=True)
@click.command()
@click.option('--read','-r','readOnly',help='Read mode', required=False, is_flag=True, show_default=True, default=False)
@click.option('--role','-rl','roleName',help='Snowflake role to be assigned to perform this task',required=True,show_default=True,default='CDOPS_ADMIN')
@click.option('--warehouse','-w','warehouse',help='Snowflake Warehouse',required=True,default="CDOPS_WH")
@click.option('--cdafile','-f','cdafile',help='Control Data Access File Location',required=True,default="cda.csv")
@click.option('--table','-t','table',help='Fully Qualified table name <DATABASE.SCHEMA.TABLE>',required=False, default='CDOPS_STATESTORE.REPORTING.MEMBER_RESOURCE_MAPPING', show_default=True)
@click.option('--account',
              '-a','account',
              help='Snowflake account name',
              required=True)
@click.option('--privateKey',
              '-pk','privateKey',
              help='Private Key absolute path if connTpe selected is KEY. If password proctected set enviornment variable PRIVATE_KEY_PASSPHRASE',
              required=False)
@click.option('--connType',
              '-c',
              'connType',
              help='Type of connection. BASIC=USERID/PASSWORD SSO=WEB_SSO_LOGIN KEY=PRIVATE KEY',
              type=click.Choice(('BASIC', 'SSO', 'KEY'),case_sensitive=False),
              required=True)
@click.option('--userID',
              '-u',
              'userID',
              help='UserID if connType selected is BASIC/SSO/KEY. Password should be set via enviornment variable SNOWSQL_PWD if connType is BASIC.',
              required=False)
@pass_context
def main(ctx,account,privateKey,connType,userID,readOnly,roleName,warehouse,table,cdafile):
    ctx.account = account
    ctx.privateKey = privateKey
    ctx.connType = connType
    ctx.userID = userID
    ctx.readOnly = readOnly
    ctx.roleName = roleName
    ctx.warehouse = warehouse
    ctx.table = table
    ctx.cdafile = cdafile

    if ctx.readOnly: print("##### Read Mode Enabled #####")

    try:
        ctx.initializeLogger()
        ctx.initializeConnection()
        ctx.processControlDataAccess()
    except:
        ctx.logger.error(traceback.print_exc(file=sys.stdout))
        sys.exit(1)
    finally:
        ctx.releaseResource()

if __name__=='__main__':
    main()
