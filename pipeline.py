import functions.databaseCreator as creator
import functions.dataImporter as dataImporter
import functions.databaseImporter as dbImporter
import functions.dataChecker as checker

creator.delete_tables()  
creator.create_tables()
creator.delete_tables('_test')
creator.create_tables('_test')
dbImporter.import_database()
dataImporter.import_data()
checker.check_data()