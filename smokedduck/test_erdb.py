import time
import pandas as pd
import smokedduck

con = smokedduck.connect()
con.execute(f'CALL dbgen(sf=20);')
con.execute('pragma threads=16')
tables = con.execute(f'pragma show_tables').df()
print(tables)

q1 = f"""select l_quantity as x, sum(2.0*l_extendedprice - l_extendedprice) as sy,
count(l_extendedprice - l_extendedprice) as cy
from lineitem group by l_quantity"""
q2 = f"""select ( (count(r.x)*sum(r.x*(r.sy/r.cy))) - (sum(r.x)*sum(r.sy/cy)) ) / ((count(r.x)*(sum(r.x*r.x)) - (sum(r.x))*(sum(r.x))))
from q1_out as r"""
q = f"""with r as ({q1})
select ( (count(r.x)*sum(r.x*(r.sy/r.cy))) - (sum(r.x)*sum(r.sy/r.cy)) ) / ((count(r.x)*(sum(r.x*r.x)) - (sum(r.x))*(sum(r.x))))
from r"""
out = con.execute(q).df()
print(out)

# 1. break the query down
q1_out = con.execute(q1).df()
print(q1_out)
q2_out = con.execute(q2).df()
print(q2_out)

start = time.time()
# update q1
q1_exp = f"""select l_partkey as e2,l_quantity as x,
sum(2.0*l_extendedprice - l_extendedprice) as sy,
count(l_extendedprice - l_extendedprice) as cy
from lineitem group by l_quantity, l_partkey"""
q1_exp_out_diff = con.execute(q1_exp).df()

q1_exp_out = con.execute(f"""select diff.e2, x, base.sy-diff.sy as sy, base.cy-diff.cy as cy
from q1_exp_out_diff as diff JOIN q1_out as base using (x)""").df()


q = f"""
select e2, ( (count(r.x)*sum(r.x*r.sy/r.cy)) - (sum(r.x)*sum(r.sy/r.cy)) ) / ((count(r.x)*(sum(r.x*r.x)) - (sum(r.x))*(sum(r.x)))) as slope
from q1_exp_out as r
group by e2
order by slope
limit 3"""
out = con.execute(q).df()
end = time.time()


print(q1_exp_out)
print(q1_exp_out_diff)
print(out)

print(end - start)
print(con.execute("select count() from lineitem").df())
