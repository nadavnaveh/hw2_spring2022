# from types import NoneType
from typing import List
import Utility.DBConnector as Connector
from Utility.Status import Status
from Utility.Exceptions import DatabaseException
from Business.File import File
from Business.RAM import RAM
from Business.Disk import Disk
from psycopg2 import sql


def createTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("BEGIN;\
                            CREATE TABLE Files (FileID INTEGER NOT NULL PRIMARY KEY,\
                                                FileType TEXT NOT NULL,\
                                                DiskSizeNeeded INTEGER NOT NULL,\
                                                CHECK(FileID > 0),\
                                                CHECK(DiskSizeNeeded >= 0));\
                             CREATE TABLE Disks (DiskID INTEGER NOT NULL PRIMARY KEY,\
                                                 DiskManufacturer TEXT NOT NULL,\
                                                 DiskSpeed INTEGER NOT NULL,\
                                                 DiskFreeSpace INTEGER NOT NULL,\
                                                 DiskCostPerByte INTEGER NOT NULL,\
                                                 CHECK(DiskID > 0),\
                                                 CHECK(DiskSpeed > 0),\
                                                 CHECK(DiskCostPerByte > 0),\
                                                 CHECK(DiskFreeSpace >= 0));\
                             CREATE TABLE Rams (RamID INTEGER NOT NULL PRIMARY KEY,\
                                                RamSize INTEGER NOT NULL,\
                                                RamCompany TEXT NOT NULL,\
                                                CHECK(RamID > 0),\
                                                CHECK(RamSize > 0));\
                             CREATE TABLE RamsXDisks (RamID INTEGER NOT NULL,\
                                                      DiskID INTEGER NOT NULL,\
                                                      RamSize INTEGER,\
                                                      PRIMARY KEY(RamID, DiskID),\
                                                      FOREIGN KEY (RamID) REFERENCES Rams(RamID) ON DELETE CASCADE,\
                                                      FOREIGN KEY (DiskID) REFERENCES Disks(DiskID) ON DELETE CASCADE,\
                                                      CHECK(RamSize >= 0));\
                             CREATE TABLE FilesXDisks (FileID INTEGER NOT NULL,\
                                                       DiskID INTEGER NOT NULL,\
                                                       Cost INTEGER, \
                                                       PRIMARY KEY(FileID, DiskID),\
                                                       FOREIGN KEY (FileID) REFERENCES Files(FileID) ON DELETE CASCADE,\
                                                       FOREIGN KEY (DiskID) REFERENCES Disks(DiskID) ON DELETE CASCADE,\
                                                       CHECK(Cost >= 0));\
                             CREATE VIEW DiskFileCount AS (SELECT DiskID, DiskSpeed,\
                                                          (SELECT COUNT(FileID) FROM Files AS f\
                                                           WHERE f.DiskSizeNeeded<=d.DiskFreeSpace)\
                                                           AS FileCount FROM disks AS d);\
                             CREATE VIEW FilesStorableOnDisks AS (SELECT d.DiskID, f.FileID,f.DiskSizeNeeded\
                                                                  FROM Disks AS d, Files AS f\
                                                                  WHERE DiskSizeNeeded <= DiskFreeSpace);\
                        COMMIT;")
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()


def clearTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("BEGIN;\
                             DELETE FROM Files;\
                             DELETE FROM Disks;\
                             DELETE FROM Rams;\
                             DELETE FROM FilesXDisks;\
                             DELETE FROM RamsXDisks;\
                          COMMIT;")
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()


def dropTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("BEGIN;\
                             DROP TABLE IF EXISTS Files CASCADE;\
                             DROP TABLE IF EXISTS Disks CASCADE;\
                             DROP TABLE IF EXISTS Rams CASCADE;\
                             DROP TABLE IF EXISTS FilesXDisks CASCADE;\
                             DROP TABLE IF EXISTS RamsXDisks CASCADE;\
                             DROP VIEW IF EXISTS FilesStorableOnDisks;\
                          COMMIT;")
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()


def addFile(file: File) -> Status:
    stat = Status.OK
    conn = None
    try:
        conn = Connector.DBConnector()
        if conn==None:
            print("Database did not connect. I think.")     #remove this line later
        q = sql.SQL("INSERT INTO Files(FileID, FileType, DiskSizeNeeded) VALUES({file_id},{file_type},"
                    "{disk_size_needed})").format(file_id=sql.Literal(file.getFileID()),
                                                  file_type=sql.Literal(file.getType()),
                                                  disk_size_needed=sql.Literal(file.getSize()))
        rows_effected, _ = conn.execute(q)
        conn.commit()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        stat = Status.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        stat = Status.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        stat = Status.ALREADY_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        status = Status.ERROR
    except Exception as e:
        stat = Status.ERROR
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    return stat


def getFileByID(fileID: int) -> File:
    conn = None
    rows_effected, ret_val = 0, None
    try:
        conn = Connector.DBConnector()
        q = sql.SQL("SELECT * FROM Files WHERE fileID = {file_id}").format(file_id=sql.Literal(fileID))
        rows_effected, ret_val = conn.execute(q)
        conn.commit()
    except Exception as e:
        print(e)
    finally:
        conn.close()
        if rows_effected == 0:
            return File.badFile()
        return File(ret_val[0]['FileID'], ret_val[0]['FileType'], ret_val[0]['DiskSizeNeeded'])


def deleteFile(file: File) -> Status:
    stat = Status.OK
    conn = None
    try:
        conn = Connector.DBConnector()
        q = sql.SQL("BEGIN;\
                     UPDATE Disks SET DiskFreeSpace = DiskFreeSpace + {file_size}\
                     WHERE DiskID IN (SELECT DiskID FROM FilesXDisks WHERE FileID={id});\
                     DELETE FROM Files WHERE FileID={id};\
                     COMMIT;").format(id=sql.Literal(file.getFileID()), file_size=sql.Literal(file.getSize()))
        conn.execute(q)
        conn.commit()
    except Exception as e:
        stat = Status.ERROR
        print(e)
    finally:
        conn.close()
        return stat


def addDisk(disk: Disk) -> Status:
    stat = Status.OK
    conn = None
    try:
        conn = Connector.DBConnector()
        q = sql.SQL("INSERT INTO Disks(DiskID, DiskManufacturer, DiskSpeed, DiskFreeSpace, DiskCostPerByte)\
                    VALUES({disk_id},{disk_man}, {disk_speed}, {disk_free_space}, {disk_cost_per_byte})") \
            .format(disk_id=sql.Literal(disk.getDiskID()),
                    disk_man=sql.Literal(disk.getCompany()),
                    disk_speed=sql.Literal(disk.getSpeed()),
                    disk_free_space=sql.Literal(disk.getFreeSpace()),
                    disk_cost_per_byte=sql.Literal(disk.getCost()))
        row_affected, _ = conn.execute(q)
        conn.commit()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        stat = Status.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        stat = Status.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        stat = Status.ALREADY_EXISTS
    except Exception as e:
        stat = Status.ERROR
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    return stat


def getDiskByID(diskID: int) -> Disk:
    rows_effected, ret_val = 0, None
    conn = None
    try:
        conn = Connector.DBConnector()
        q = sql.SQL("SELECT * FROM Disks WHERE DiskID = {disk_id}").format(disk_id=sql.Literal(diskID))
        rows_effected, ret_val = conn.execute(q)
        conn.commit()
    except Exception as e:
        print(e)
    finally:
        conn.close()
        if rows_effected == 0:
            return Disk.badDisk()
        return Disk(ret_val[0]['DiskID'],
                    ret_val[0]['DiskManufacturer'],
                    ret_val[0]['DiskSpeed'],
                    ret_val[0]['DiskFreeSpace'],
                    ret_val[0]['DiskCostPerByte'])


def deleteDisk(diskID: int) -> Status:
    stat = Status.OK
    conn = None
    try:
        conn = Connector.DBConnector()
        q = sql.SQL("DELETE FROM Disks WHERE DiskID = {disk_id}").format(disk_id=sql.Literal(diskID))
        row_affected, _ = conn.execute(q)
        conn.commit()
        if not row_affected:
            stat = Status.NOT_EXISTS
    except Exception as e:
        stat = Status.ERROR
    finally:
        conn.close()
    return stat


def addRAM(ram: RAM) -> Status:
    stat = Status.OK
    conn = None
    try:
        conn = Connector.DBConnector()
        q = sql.SQL("INSERT INTO Rams(RamID, RamSize, RamCompany) VALUES({ram_id},{ram_size},"
                    "{ram_company})").format(ram_id=sql.Literal(ram.getRamID()),
                                             ram_size=sql.Literal(ram.getSize()),
                                             ram_company=sql.Literal(ram.getCompany()))
        rows_effected, _ = conn.execute(q)
        conn.commit()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        stat = Status.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        stat = Status.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        stat = Status.ALREADY_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        status = Status.ERROR
    except Exception as e:
        stat = Status.ERROR
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    return stat


def getRAMByID(ramID: int) -> RAM:
    rows_effected, ret_val = 0, None
    conn = None
    try:
        conn = Connector.DBConnector()
        q = sql.SQL("SELECT * FROM Rams WHERE ramID = {ram_id}").format(ram_id=sql.Literal(ramID))
        rows_effected, ret_val = conn.execute(q)
    except Exception as e:
        print(e)
    finally:
        conn.close()
        if rows_effected == 0:
            return RAM.badRAM()
        return RAM(ret_val[0]['RamID'], ret_val[0]['RamCompany'], ret_val[0]['RamSize'])


def deleteRAM(ramID: int) -> Status:
    stat = Status.OK
    conn = None
    try:
        conn = Connector.DBConnector()
        q = sql.SQL("DELETE FROM Rams WHERE RamID = {ram_id}").format(ram_id=sql.Literal(ramID))
        row_affected, _ = conn.execute(q)
        conn.commit()
        if not row_affected:
            stat = Status.NOT_EXISTS
    except Exception as e:
        stat = Status.ERROR
    finally:
        conn.close()
    return stat


def addDiskAndFile(disk: Disk, file: File) -> Status:
    stat = Status.OK
    conn = None
    try:
        conn = Connector.DBConnector()
        q = sql.SQL("BEGIN;\
                    INSERT INTO Disks(DiskID, DiskManufacturer, DiskSpeed, DiskFreeSpace, DiskCostPerByte)\
                    VALUES({disk_id},{disk_man}, {disk_speed}, {disk_free_space}, {disk_cost_per_byte}); \
                    INSERT INTO Files(FileID, FileType, DiskSizeNeeded)\
                    VALUES({file_id},{file_type},{disk_size_needed});\
                    COMMIT;").format(disk_id=sql.Literal(disk.getDiskID()),
                                     disk_man=sql.Literal(disk.getCompany()),
                                     disk_speed=sql.Literal(disk.getSpeed()),
                                     disk_free_space=sql.Literal(disk.getFreeSpace()),
                                     disk_cost_per_byte=sql.Literal(disk.getCost()),
                                     file_id=sql.Literal(file.getFileID()),
                                     file_type=sql.Literal(file.getType()),
                                     disk_size_needed=sql.Literal(file.getSize())
                                     )
        row_affected, _ = conn.execute(q)
        conn.commit()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        stat = Status.BAD_PARAMS
        # conn.rollback()
    except DatabaseException.CHECK_VIOLATION as e:
        stat = Status.BAD_PARAMS
        # conn.rollback()
    except DatabaseException.UNIQUE_VIOLATION as e:
        stat = Status.ALREADY_EXISTS
        # conn.rollback()
    except Exception as e:
        stat = Status.ERROR
        # conn.rollback()
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    return stat


def addFileToDisk(file: File, diskID: int) -> Status:
    stat = Status.OK
    conn = None
    try:
        conn = Connector.DBConnector()
        q = sql.SQL("BEGIN;\
                    INSERT INTO FilesXDisks(FileID, DiskID, Cost)\
                    VALUES ({file_id}, {disk_id},\
                    (SELECT DiskSizeNeeded from Files WHERE FileID = {file_id})*(SELECT DiskCostPerByte\
                    FROM Disks WHERE DiskID = {disk_id}));\
                    UPDATE Disks SET DiskFreeSpace = DiskFreeSpace - {file_size} WHERE DiskID = {disk_id};\
                    COMMIT;").format(file_id=sql.Literal(file.getFileID()),
                                     disk_id=sql.Literal(diskID),
                                     file_size=sql.Literal(file.getSize()))
        rows_effected, _ = conn.execute(q)
        conn.commit()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        stat = Status.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        stat = Status.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        stat = Status.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        stat = Status.NOT_EXISTS
    except Exception as e:
        stat = Status.ERROR
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    return stat


def removeFileFromDisk(file: File, diskID: int) -> Status:
    stat = Status.OK
    conn = None
    try:
        conn = Connector.DBConnector()
        q = sql.SQL("BEGIN;\
                        UPDATE Disks SET DiskFreeSpace = DiskFreeSpace + {file_size} WHERE DiskID =\
                        (SELECT DiskID FROM FilesXDisks WHERE FileID={file_id} AND DiskID={disk_id});\
                        DELETE FROM FilesXDisks WHERE DiskID = {disk_id} AND FileID = {file_id};\
                        COMMIT;").format(file_id=sql.Literal(file.getFileID()),
                                         disk_id=sql.Literal(diskID),
                                         file_size=sql.Literal(file.getSize()))
        rows_effected, _ = conn.execute(q)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        stat = Status.BAD_PARAMS
        # conn.rollback()
    except Exception as e:
        stat = Status.ERROR
        print(e)
        # conn.rollback()
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    return stat


def addRAMToDisk(ramID: int, diskID: int) -> Status:
    stat = Status.OK
    conn = None
    try:
        conn = Connector.DBConnector()
        q = sql.SQL("INSERT INTO RamsXDisks(RamID, DiskID, RamSize)\
                     VALUES ({ram_id}, {disk_id},\
                     (SELECT RamSize FROM Rams WHERE RamID = {ram_id}))").format(ram_id=sql.Literal(ramID),
                                                                                 disk_id=sql.Literal(diskID))
        rows_effected, _ = conn.execute(q)
        conn.commit()
        if rows_effected == 0:
            res = Status.NOT_EXISTS
    except DatabaseException.NOT_NULL_VIOLATION as e:
        stat = Status.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        stat = Status.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        stat = Status.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        stat = Status.NOT_EXISTS
    except Exception as e:
        stat = Status.ERROR
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    return stat


def removeRAMFromDisk(ramID: int, diskID: int) -> Status:
    stat = Status.OK
    conn = None
    try:
        conn = Connector.DBConnector()
        q = sql.SQL("DELETE FROM RamsXDisks\
                     WHERE DiskID = {disk_id}\
                     AND RamID = {ram_id}").format(ram_id=sql.Literal(ramID),
                                                    disk_id=sql.Literal(diskID))
        rows_effected, _ = conn.execute(q)
        conn.commit()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        stat = Status.BAD_PARAMS
        # conn.rollback()
    except Exception as e:
        stat = Status.ERROR
        # conn.rollback()
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
        if stat == Status.OK and not rows_effected:
            stat = Status.NOT_EXISTS
    return stat



def averageFileSizeOnDisk(diskID: int) -> float:
    conn = None
    avg = 0
    try:
        conn = Connector.DBConnector()
        query = sql.SQL('SELECT AVG(DiskSizeNeeded)\
                        FROM Files WHERE FileID\
                        IN (SELECT FileID FROM FilesXDisks WHERE DiskID={disk_id})').format(disk_id=sql.Literal(diskID))
        _, result = conn.execute(query)
        avg = result[0]['avg']
        if avg is None:
            avg = 0
        conn.commit()
    except Exception as e:
        avg = -1
    finally:
        conn.close()
        return avg

def diskTotalRAM(diskID: int) -> int:
    conn = None
    s = 0
    try:
        conn = Connector.DBConnector()
        q = sql.SQL("SELECT SUM(RamSize) FROM RamsXDisks\
                     WHERE DiskID = {disk_id}").format(disk_id=sql.Literal(diskID))
        # in case there is no disk with id diskID the coalesce will return 0, which is
        # the value required in case there are no disks with that Id.
        _, res_set = conn.execute(q)
        s = res_set[0]['sum']
        if s is None:
            s = 0
        conn.commit()
    except Exception as e:
        print(e)
        s = -1
    finally:
        conn.close()
        return s


def getCostForType(type: str) -> int:
    conn = None
    cost = 0
    try:
        conn = Connector.DBConnector()
        q = sql.SQL("SELECT SUM(Cost)\
                     From (SELECT * FROM Files WHERE FileType={type}) AS\
                     TypeFiles INNER JOIN FilesXDisks ON TypeFiles.FileID = FilesXDisks.FileID").format(type=sql.Literal(type))
        _, res_set = conn.execute(q)
        cost = res_set[0]['sum']
        if cost is None:
            cost = 0
        conn.commit()
    except Exception as e:
        print(e)
        cost = -1
    finally:
        conn.close()
        return cost


def getFilesCanBeAddedToDisk(diskID: int) -> List[int]:
    conn = None
    ret_lst = []
    try:
        conn = Connector.DBConnector()
        q = sql.SQL(
            "SELECT FileID FROM FilesStorableOnDisks WHERE DiskID = {diskID} ORDER BY FileID DESC LIMIT 5").format(
            diskID=sql.Literal(diskID))
        _, res_set = conn.execute(q, printSchema=False)
        conn.commit()
        # print users
        for index in res_set.rows:
            current_row = index[0]
            ret_lst.append(current_row)
    except Exception as e:
        print(e)
    finally:
        conn.close()
    return ret_lst


def getFilesCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    conn = None
    ret_lst = []
    try:
        conn = Connector.DBConnector()
        q = sql.SQL("SELECT FileID FROM FilesStorableOnDisks\
                     WHERE DiskId = {disk_id}\
                     AND (DiskSizeNeeded <= (SELECT COALESCE(SUM(RamSize),0) FROM RamsXDisks WHERE DiskID = {disk_id}))\
                     ORDER BY FileID ASC LIMIT 5").format(disk_id=sql.Literal(diskID))
        _, res_set = conn.execute(q)
        conn.commit()
        for index in res_set.rows:
            current_row = index[0]
            ret_lst.append(current_row)
    except Exception as e:
        print(e)
        ret_lst = []
    finally:
        conn.close()
    return ret_lst


def isCompanyExclusive(diskID: int) -> bool:
    res = False
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT DISTINCT * FROM ((SELECT RamCompany FROM Rams WHERE\
             (RamID IN (SELECT RamID FROM RamsXDisks WHERE DiskID = {diskID}))) as ramco\
              FULL OUTER JOIN (SELECT DiskManufacturer FROM Disks WHERE DiskID = {diskID}) as disco\
              ON ramco.RamCompany = disco.DiskManufacturer) as a").format(
            diskID=sql.Literal(diskID))
        rows_effected, _ = conn.execute(query)
        conn.commit()
        if rows_effected == 1:
            res = True
    except Exception as e:
        res = False
    finally:
        conn.close()
    return res



def getConflictingDisks() -> List[int]:
    conn = None
    res_lst = []
    try:
        conn = Connector.DBConnector()
        q = sql.SQL("SELECT DISTINCT DiskID FROM FilesXDisks\
                     WHERE FileID IN(SELECT FileID FROM FilesXDisks GROUP BY FileID HAVING COUNT(FileID) > 1)\
                     ORDER BY DiskID ASC")
        rows_effected, res_set = conn.execute(q)
        conn.commit()
        for i in res_set.rows:
            res_lst.append(i[0])
    except Exception as e:
        print(e)
        res_lst = []
    finally:
        conn.close()
    return res_lst


def mostAvailableDisks() -> List[int]:
    conn = None
    res_lst = []
    try:
        conn = Connector.DBConnector()
        q = sql.SQL('SELECT DiskID FROM DiskFileCount ORDER BY FileCount DESC, DiskSpeed DESC, DiskID ASC LIMIT 5')
        rows_effected, res_set = conn.execute(q)
        for i in range(0, rows_effected):
            res_lst.append(res_set[i]['DiskID'])
        conn.commit()
    except Exception as e:
        pass
    finally:
        conn.close()
        return res_lst


def getCloseFiles(fileID: int) -> List[int]:
    conn = None
    res = []
    try:
        conn = Connector.DBConnector()
        q = sql.SQL("SELECT FileID FROM (SELECT * FROM (SELECT * FROM FilesXDisks WHERE\
                     FileID != {file_id} AND DiskID IN\
                     (SELECT DiskID FROM FilesXDisks WHERE FileID = {file_id})) AS helper\
                      FULL JOIN Files USING(FileID)) AS FullJoin\
                      WHERE FileID != {file_id}\
                      GROUP BY FileID HAVING COALESCE(COUNT(DiskID),0) >= (SELECT (COUNT(*)+1)/2\
                      FROM FilesXDisks WHERE FileID = {file_id})\
                      ORDER BY FileID ASC LIMIT 10").format(file_id=sql.Literal(fileID))
        rows_effected, res_set = conn.execute(q)
        conn.commit()
        for i in res_set.rows:
            res.append(i[0])
    except Exception as e:
        res = []
    finally:
        conn.close()
    return res


#debugging functions:
# def getFiles():
#     conn = None
#     ret_lst = []
#     try:
#         conn = Connector.DBConnector()
#         q = sql.SQL("SELECT * FROM Files")
#         _, res_set = conn.execute(q)
#         conn.commit()
#         #for index in res_set.rows:
#          #   current_row = index[0]
#          #   ret_lst.append(current_row)
#     except Exception as e:
#         print(e)
#         res = []
#     finally:
#         conn.close()
#     return res_set