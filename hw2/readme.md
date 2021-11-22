## environment setup
1. Prepare the multiple machine
2. Build a Spark Cluster
4. '/opt/spark/sbin/start-all.sh'
3. 'pip install -r requirements.txt'
4. datasets put in './datasets'
5. p.s. every worker also need put datasets

## complie
1. Open 109598033_109598001_hw2.ipynb
2. Cell -> run all
3. Wait about 6 minutes

## Result
1. csv file all in 'outputs'
2. p.s.Use spark.write.csv output file might repartition to worker machine, so use .toPandas().to_csv to write output files