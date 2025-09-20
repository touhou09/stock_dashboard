bronze layer에서는 미국 배당주 데이터의 delta table과 snp500의 데이터를 갖도록 한다.

silver layer는 각 snp500 데이터의 종목코드,이름,날짜,가격정보들,배당유무 등을 저장하도록 한다

gold layer는 별도로 만들기 보다는 bigquery의 biglake를 활용해 view로 처리하거나 duckdb를 이용해 view처럼 관리할 수 있도록 한다.
