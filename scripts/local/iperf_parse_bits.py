import json,sys
d=json.load(sys.stdin)
kind=sys.argv[1]
if kind=='tcp':
    s=d['end'].get('sum_received') or d['end'].get('sum')
    print(round(s['bits_per_second']/1e9,3))
else:
    r=d['end']['sum_received']
    print(f"{r['bits_per_second']/1e6:.1f}/{float(r.get('lost_percent') or 0):.2f}%")
