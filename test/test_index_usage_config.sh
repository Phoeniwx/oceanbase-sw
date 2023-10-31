# SELECT * FROM oceanbase.DBA_OB_TENANTS
echo '---------------show sys tenant----------------------'
mysql -uroot@sys -h127.0.0.1 -P10400 -Doceanbase -t <<EOF
SELECT NAME,VALUE FROM oceanbase.GV\$OB_PARAMETERS where name like '_iut%';
EOF
echo '---------------show mysql tenant----------------------'
mysql -uroot@mysql -h127.0.0.1 -P10400 -Doceanbase -t <<EOF
SELECT NAME,VALUE FROM oceanbase.GV\$OB_PARAMETERS where name like '_iut%';
EOF
echo '--------------set sys tenant--------------'
mysql -uroot@sys -h127.0.0.1 -P10400 -Doceanbase -t <<EOF
ALTER SYSTEM SET _iut_stat_collection_type ='ALL' TENANT='sys';
ALTER SYSTEM SET _iut_enable=0 TENANT='sys';
ALTER SYSTEM SET _iut_max_entries=2000 TENANT='sys';
SELECT NAME,VALUE FROM oceanbase.GV\$OB_PARAMETERS where name like '_iut%';
EOF
echo '-------------show mysql tenant-------------'
mysql -uroot@mysql -h127.0.0.1 -P10400 -Doceanbase -t <<EOF
SELECT NAME,VALUE FROM oceanbase.GV\$OB_PARAMETERS where name like '_iut%';
EOF

echo '--------------recover sys tenant--------------'
mysql -uroot@sys -h127.0.0.1 -P10400 -Doceanbase -t <<EOF
ALTER SYSTEM SET _iut_stat_collection_type ='SAMPLE' TENANT='sys';
ALTER SYSTEM SET _iut_enable=1 TENANT='sys';
ALTER SYSTEM SET _iut_max_entries=30000 TENANT='sys';
SELECT NAME,VALUE FROM oceanbase.GV\$OB_PARAMETERS where name like '_iut%';
EOF

echo '---------------show sys tenant----------------------'
mysql -uroot@sys -h127.0.0.1 -P10400 -Doceanbase -t <<EOF
SELECT NAME,VALUE FROM oceanbase.GV\$OB_PARAMETERS where name like '_iut%';
EOF

