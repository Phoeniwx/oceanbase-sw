echo '---------------init test data----------------------'
mysql -uroot@mysql -h127.0.0.1 -P10400 -Dtest -t <<EOF
drop table if exists test_table;
CREATE TABLE test_table (
    id INT PRIMARY KEY AUTO_INCREMENT,
    column1 INT,
    column2 VARCHAR(50),
    column3 FLOAT,
    column4 DATE,
    INDEX index1 (column1),
    INDEX index2 (column2),
    INDEX index3 (column3),
    INDEX index4 (column4)
);

INSERT INTO test_table (column1, column2, column3, column4)
VALUES
    (1, 'Data 1', 1.23, '2022-01-01'),
    (2, 'Data 2', 4.56, '2022-01-02'),
    (3, 'Data 3', 7.89, '2022-01-03'),
    (4, 'Data 3', 7.89, '2022-01-03'),
    (3, 'Data 3', 7.89, '2022-01-03'),
    (6, 'Data 3', 7.89, '2022-01-03'),
    (6, 'Data 3', 7.89, '2022-01-03'),
    (6, 'Data 3', 7.89, '2022-01-03'),
    (6, 'Data 3', 7.89, '2022-01-03'),
    (3, 'Data 3', 7.89, '2022-01-03'),
    (7, 'Data 3', 7.89, '2022-01-03'),
    (3, 'Data 3', 7.89, '2022-01-03'),
    (11, 'Data 3', 7.89, '2022-01-03'),
    (99, 'Data 99', 99.99, '2022-04-09'),
    (100, 'Data 100', 100.00, '2022-04-10');

select * from test_table group by column1 having column1=6;

select * from test_table where column2='Data 3';

select * from test_table where column2='Data 1';
EOF
