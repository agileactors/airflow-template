

CREATE SCHEMA IF NOT EXISTS test_schema;

CREATE TABLE IF NOT EXISTS test_schema.test_table (
     id Int not Null,
	 last_transaction_date Varchar(200),
	 description Varchar(200)
	);


delete from test_schema.test_table;
insert into  test_schema.test_table values (1, '1968-10-23', 'buy 1');
insert into  test_schema.test_table values (2, '1968-10-24', 'buy 2');