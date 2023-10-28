echo '---------------query mock tenant----------------------'
for i in $(seq 1 100); do
mysql -uroot@mysql -h127.0.0.1 -P10400 -Dtest -t <<EOF
select * from test_table where column2='Data 1';
EOF
done
