import functions.databaseCreator as creator
import functions.dataImporter as dataImporter
import functions.databaseImporter as dbImporter
import functions.dataChecker as checker

creator.create_tables()
dbImporter.import_database()
dataImporter.import_data()
checker.check_data()