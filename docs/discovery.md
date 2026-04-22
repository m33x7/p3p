| Discovery protocol feature                | Bootstrap discovery V1                     |
| -----------                               | -----------                                |
| `Low bootstrap dependence`                | `MID`<br>still depend on bootstrap node    |
| `Decentralized peer sampling`             | `MID`<br>gossip, but no random walks       |
| `Resilience to churn`                     | `MID`<br>no topology analysis              |
| `Efficient propagation (bounded gossip)`  | `MID`<br>no controlled fanout              |
| `Local view, global emergence`            | `LOW`<br>Peer knows about everybody        |
| `Sybil resistance (at least mild)`        | `LOW`<br>no protection                     |
| `NAT / network reality awareness`         | `LOW`<br>at least we tell the node what its connection looks from the outside and store node addresses.|
| `Multi-path redundancy`                   | `LOW`<br>no topology analysis              |
| `Fast convergence + low overhead tradeoff`| `LOW`<br>no controlled fanout + knowledge about whole network|
| `Graceful degradation`                    | `MID`<br>                                  |
----------- 

`Low bootstrap dependence`
You should not rely on a single server or fixed peers. Bootstrap nodes help you enter the network, but they must not be critical infrastructure.

`Decentralized peer sampling`
Nodes should continuously exchange partial views of the network (gossip / random walks), so knowledge spreads organically instead of centrally.

`Resilience to churn`
Peers come and go constantly. Discovery must assume the network is unstable and continuously refresh routes and peer tables.

`Efficient propagation (bounded gossip)`
Information spreads fast but not explosively. Controlled fanout prevents bandwidth collapse while still achieving coverage.

`Local view, global emergence`
Each node only knows a small subset of peers, but collectively the system approximates global connectivity. No node should need full topology knowledge (because that would be adorable and impossible).

`Sybil resistance (at least mild)`
Discovery must not be trivially polluted by fake nodes flooding identities. Even lightweight rate limits, scoring, or reputation help.

`NAT / network reality awareness`
Direct connectivity is not guaranteed. You need relays, hole punching, or rendezvous systems built into discovery assumptions.

`Multi-path redundancy`
Multiple independent routes to find the same peer/service. If one path dies, the network doesn’t implode dramatically.

`Fast convergence + low overhead tradeoff`
You want nodes to learn “enough of the network” quickly without turning discovery traffic into its own DDoS simulation.

`Graceful degradation`
Even if half the network disappears or partitions, the remaining parts should still function locally. Convergence after recovery is possible. But global reconvergence is not guaranteed.