from Solution import *

dropTables()
createTables()
addFile(File(2, "HOMO", 1))
addDisk(Disk(1, 'a', 2, 3, 4))
print(addRAM(RAM(10, 'a', 20)))
