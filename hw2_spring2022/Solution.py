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
                             CREATE TABLE FilesXDisks\
                                 (FileID INTEGER REFERENCES Files\
                                  ON DELETE CASCADE,\
                                  DiskID INTEGER REFERENCES Disks\
                                  ON DELETE CASCADE,\
                                  Cost INTEGER, \
                                  PRIMARY KEY(FileID, DiskID),\
                                  CHECK(Cost >= 0));\
                             CREATE TABLE RamsXDisks\
                                 (RamID INTEGER REFERENCES Rams\
                                  ON DELETE CASCADE,\
                                  DiskID INTEGER REFERENCES Disks\
                                  ON DELETE CASCADE,\
                                  RamSize INTEGER,\
                                  PRIMARY KEY(RamID, DiskID),\
                                  CHECK(RamSize >= 0));\
                             CREATE VIEW FilesCanBeAddedOnDisks AS \
                             (SELECT F.FileID, D.DiskID, F.DiskSizeNeeded FROM Files F, Disks D WHERE DiskSizeNeeded <= DiskFreeSpace);\
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
                             DROP VIEW IF EXISTS FilesCanBeAddedOnDisks;\
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
    except Exception as e:
        stat = Status.ERROR
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    return stat


def getFileByID(fileID: int) -> File:
    file_to_ret = File.badFile()
    conn = None
    try:
        conn = Connector.DBConnector()
        q = sql.SQL("SELECT * FROM Files WHERE fileID = {file_id}").format(file_id=sql.Literal(fileID))
        _, ret_val = conn.execute(q)
        val = ret_val.__getitem__(0)
        file_to_ret = File(val[ret_val.cols_header[0]], val[ret_val.cols_header[1]], val[ret_val.cols_header[2]])
    except Exception as e:
        print(e)
    finally:
        conn.close()
    return file_to_ret


def deleteFile(file: File) -> Status:
    stat = Status.OK
    conn = None
    try:
        conn = Connector.DBConnector()
        q = sql.SQL("DELETE FROM Files WHERE FileID = {file_id}").format(file_id=sql.Literal(file.getFileID()))
        row_affected, _ = conn.execute(q)
        conn.commit()
        if not row_affected:
            stat = Status.NOT_EXISTS
    except Exception as e:
        stat = Status.ERROR
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
    disk_to_ret = File.badFile()
    conn = None
    try:
        conn = Connector.DBConnector()
        q = sql.SQL("SELECT * FROM Disks WHERE DiskID = {disk_id}").format(disk_id=sql.Literal(diskID))
        _, ret_val = conn.execute(q)
        val = ret_val.__getitem__(0)
        disk_to_ret = Disk(val[ret_val.cols_header[0]], val[ret_val.cols_header[1]], val[ret_val.cols_header[2]],
                           val[ret_val.cols_header[3]], val[ret_val.cols_header[4]])
    except Exception as e:
        print(e)
    finally:
        conn.close()
    return disk_to_ret


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
    except Exception as e:
        stat = Status.ERROR
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    return stat


def getRAMByID(ramID: int) -> RAM:
    ram_to_ret = File.badFile()
    conn = None
    try:
        conn = Connector.DBConnector()
        q = sql.SQL("SELECT * FROM Files WHERE ramID = {ram_id}").format(ram_id=sql.Literal(ramID))
        _, ret_val = conn.execute(q)
        val = ret_val.__getitem__(0)
        ram_to_ret = RAM(val[ret_val.cols_header[0]], val[ret_val.cols_header[1]], val[ret_val.cols_header[2]])
    except Exception as e:
        print(e)
    finally:
        conn.close()
    return ram_to_ret


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
                    VALUES ({file_id}, {disk_id}, {file_size}*SELECT DiskCostPerByte\
                    FROM Disks WHERE DiskID = {disk_id});\
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
                        UPDATE Disks SET DiskFreeSpace = DiskFreeSpace + {file_size} WHERE DiskID = {disk_id};\
                        DELETE FROM FilesXDisks(FileID, DiskID, Cost) WHERE DiskID = {disk_id}) AND FileID = {file_id};\
                        COMMIT;").format(file_id=sql.Literal(file.getFileID()),
                                         disk_id=sql.Literal(diskID),
                                         file_size=sql.Literal(file.getSize()))
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
    return stat


def addRAMToDisk(ramID: int, diskID: int) -> Status:
    stat = Status.OK
    conn = None
    try:
        conn = Connector.DBConnector()
        q = sql.SQL("INSERT INTO RamsXDisks(FileID, DiskID, RamSize)\
                     VALUES ({ram_id}, {disk_id}, "
                    "SELECT RamSize FROM Rams WHERE RamID = {ram_id})").format(ram_id=sql.Literal(ramID),
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
        q = sql.SQL("DELETE FROM RamsXDisks(RamID, DiskID, RamSize) "
                    "WHERE DiskID = {disk_id}) "
                    "AND RamID = {file_id}").format(file_id=sql.Literal(ramID),
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
    return stat


def averageFileSizeOnDisk(diskID: int) -> float:
    conn = None
    average = 0
    try:
        conn = Connector.DBConnector()
        q = sql.SQL("SELECT COALESCE(AVG(DiskSizeNeeded),0) From Files "
                    "WHERE FileID IN SELECT FileID FROM FilesXDisks "
                    "WHERE DiskID = {disk_id}").format(disk_id=sql.Literal(diskID))
        _, res_set = conn.execute(q)
        conn.commit()
        average = res_set[0]['coalesce']
    except Exception as e:
        average = -1
    finally:
        conn.close()
    return average


def diskTotalRAM(diskID: int) -> int:
    conn = None
    s = 0
    try:
        conn = Connector.DBConnector()
        q = sql.SQL("SELECT COALESCE(SUM(RamSize),0) From RamXDisks "   
                    "WHERE DiskID = {disk_id}").format(disk_id=sql.Literal(diskID))
        #in case there is no disk with id diskID the coalesce will return 0, which is 
        #the value required in case there are no disks with that Id.
        _, res_set = conn.execute(q)
        conn.commit()
        s = res_set[0]['coalesce']
    except Exception as e:
        s = -1
    finally:
        conn.close()
    return s


def getCostForType(type: str) -> int:
    conn = None
    cost = 0
    try:
        conn = Connector.DBConnector()
        q = sql.SQL("SELECT COALESCE(SUM(Cost*DiskSizeNeeded),0) From (SELECT * FROM Files )\
                WHERE FileType={type}) as TypeFiles INNER JOIN  FilesXDisks ON TypeFiles.FileID=\
                    FilesXDisk.FileID"   
                    "WHERE DiskID = {disk_id}").format(disk_id=sql.Literal(type))
        _, res_set = conn.execute(q)
        conn.commit()
        sum = res_set[0]['coalesce']
    except Exception as e:
        sum = -1
    finally:
        conn.close()
    return sum


def getFilesCanBeAddedToDisk(diskID: int) -> List[int]:
    conn = None
    ret_lst = []
    try:
        conn = Connector.DBConnector()
        q = sql.SQL(
            "SELECT FileID FROM FilesCanBeAddedOnDisks WHERE DiskID = {diskID} ORDER BY FileID DESC LIMIT 5").format(
            diskID=sql.Literal(diskID))
        _, res_set = conn.execute(q, printSchema=False)
        conn.commit()
        # print users
        for index in range(res_set.size()):  # for each user
            current_row = res_set[index]["id"]  # get the row
            ret_lst.append(current_row)
    except Exception as e:
        print(e)
    finally:
        conn.close()
    return ret_lst





    return ret_lst


def getFilesCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    return []


def isCompanyExclusive(diskID: int) -> bool:
    return True


def getConflictingDisks() -> List[int]:
    return []


def mostAvailableDisks() -> List[int]:
    return []


def getCloseFiles(fileID: int) -> List[int]:
    return []
