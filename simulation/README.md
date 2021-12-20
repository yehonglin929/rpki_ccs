# How to Simulation

## Configurations
- --begin_node: Begin Node Id (Not the ASN in reality), corresponding to asn_nodeId_20211001.csv (In the format of ASN, NodeId)
- --rov: Whether use ROV
- --rov_percent: The percentage of ROV AS
- --log: Whether log res into file
- --top: ROV AS is among topXXX

```
python3 simulation.py --begin_node=0 --rov --rov_percent=0.1 --log --top=500
```