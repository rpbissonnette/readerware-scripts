What this is: a bunch of notes and code fragments for extracting data from a Readerware v3 database.

1. If you can use the native HyperSQL database, start there. See useHperSQL.md
2. If you have used Readerware to Export data to a TabSpaceVariable TSV file,
   makeSQLfromTSV.py.py might be useful.
3. If you want to try extracting all the data in the Readerware db, including
   the cover images, extractFromHyperSQL.py might be useful.
4. If you want to directly embed cover images into a database, imageResizer.py
   could be useful.  Probably you would want to delete all the abstract class
   overkill, but it does work.  
