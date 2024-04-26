import random
import argparse
import smokedduck
import pandas as pd
import numpy as np

class ZipfanGenerator(object):
    def __init__(self, n_groups, zipf_constant, card):
        self.n_groups = n_groups
        self.zipf_constant = zipf_constant
        self.card = card
        self.initZipfan()

    def zeta(self, n, theta):
        ssum = 0.0
        for i in range(0, n):
            ssum += 1. / pow(i+1, theta)
        return ssum

    def initZipfan(self):
        zetan = 1./self.zeta(self.n_groups, self.zipf_constant)
        proba = [.0] * self.n_groups
        proba[0] = zetan
        for i in range(0, self.n_groups):
            proba[i] = proba[i-1] + zetan / pow(i+1., self.zipf_constant)
        self.proba = proba

    def nextVal(self):
        uni = random.uniform(0.1, 1.01)
        lower = 0
        for i, v in enumerate(self.proba):
            if v >= uni:
                break
            lower = i
        return lower

    def getAll(self):
        result = []
        for i in range(0, self.card):
            result.append(self.nextVal())
        return result

parser = argparse.ArgumentParser()
parser.add_argument("--group", help="g", type=int, default=10)
parser.add_argument("--card", help="n", type=int, default=10000)
parser.add_argument("--alpha", help="a", type=float, default=1)
parser.add_argument("--naggs", help="naggs", type=int, default=1)
args = parser.parse_args()

con = smokedduck.connect(f'db_{args.group}_{args.card}_{args.alpha}_{args.naggs}.out')

if args.alpha == 0:
    zipfan = [random.randint(0, args.group-1) for _ in range(args.card)]
else:
    z = ZipfanGenerator(args.group, args.alpha, args.card)
    zipfan = z.getAll()

max_val = 100
unique_elements, counts = np.unique(zipfan, return_counts=True)
print(f"g={args.group}, card={args.card}, a={args.alpha}, len={len(zipfan)}")
print("1. ", len(unique_elements), unique_elements[:10], unique_elements[len(unique_elements)-10:])
print("2. ", counts[:10], counts[len(counts)-10:])
vals = np.random.uniform(0, max_val, args.card)
idx = list(range(0, args.card))
df = pd.DataFrame({'idx':idx, 'z': zipfan, 'v': vals})
con.execute("""create table micro_table as select * from df""")
print(con.execute("select * from micro_table").df())
