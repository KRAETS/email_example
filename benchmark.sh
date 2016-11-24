#!/usr/bin/env bash
mongo enron_mail_clean --eval "db.dropDatabase()"
cd Algorithm/models
rm benchmarkout/*
mkdir benchmarkout
python ImportScript1.py /Users/pedro/Downloads/enron_mail_20110402> benchmarkout/import_test1.txt 
python SetupGraphOptimized.py > benchmarkout/nokql_test1.txt 
mkdir test1nokql
mv statistics test1nokql
mv communities test1nokql
mv data.txt test1nokql
mv membership test1nokql
mv benchmarkout/* test1nokql/
python SetupGraphOptimizedKQL.py > benchmarkout/kql_test1.txt
mkdir test1
mv statistics test1
mv communities test1
mv data.txt test1
mv membership test1
mv benchmarkout/* test1/

mongo enron_mail_clean --eval "db.dropDatabase()"
