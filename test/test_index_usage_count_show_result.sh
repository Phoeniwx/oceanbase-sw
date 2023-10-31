echo '---------------show sys tenant----------------------'
mysql -uroot@sys -h127.0.0.1 -P10400 -Doceanbase -t <<EOF
SELECT * from oceanbase.__all_index_usage_info \G
EOF

echo '---------------show mysql tenant----------------------'
mysql -uroot@mysql -h127.0.0.1 -P10400 -Doceanbase -t <<EOF
SELECT NAME,VALUE FROM oceanbase.GV\$OB_PARAMETERS where name like '_iut%';
SELECT * from oceanbase.__all_index_usage_info \G
EOF
