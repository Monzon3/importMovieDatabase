import functions.databaseCreator as creator
import functions.dataImporter as dataImporter
import functions.databaseImporter as dbImporter
import functions.dataChecker as checker

creator.delete_tables()             # Delete tables, if they exist from previous testings.  
creator.create_tables()             # Generate all tables for the new MovieDB database
creator.delete_tables('_test')      # Do the same with the MovieDB_test database
creator.create_tables('_test')
dbImporter.import_database()        # Populate auxiliary tables
dataImporter.import_data()          # Import all the information into the Main table
checker.check_data()
checker.check_data('_test')