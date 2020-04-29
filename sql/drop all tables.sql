
-- https://stackoverflow.com/questions/27606518/how-to-drop-all-tables-from-a-database-with-one-sql-query/27606618

EXEC sp_msforeachtable 'ALTER TABLE ? NOCHECK CONSTRAINT all'

EXEC sp_MSforeachtable 'DROP TABLE ?'
