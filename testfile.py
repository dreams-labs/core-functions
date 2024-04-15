import sys
import os
import dreams_core as dc

print(dc.human_format(3939393272371))

test = dc.get_secret("testing_secret")
print(len(test))

query = '''
select date(created_at) as date
,count(created_at) as commands
,count(case when api_response_code = 200 then created_at end) as successful_commands
,count(case when api_response_code = 200 then created_at end)/count(created_at) as success_rate
,avg(processing_time) as avg_processing_time
,avg(dune_total_time) as avg_dune_total_time
,avg(dune_execution_time) as dune_execution_time
from etl_pipelines.logs_whale_charts
group by 1
order by 1 desc
'''

df = dc.bigquery_run_sql(query)
df.head()