import argparse
import smokedduck

parser = argparse.ArgumentParser()
parser.add_argument("--sf", help="sf", type=float, default=1)
args = parser.parse_args()
con = smokedduck.connect('db.out')
con.execute(f'CALL dbgen(sf={args.sf});')
