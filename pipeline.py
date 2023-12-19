import databaseCreator
import dataImporter
import databaseImporter
import dataChecker

databaseCreator.create_tables()
databaseImporter.import_database()
dataImporter.import_data()
dataChecker.check_data()