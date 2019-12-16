# ceph-mgr-cachetier

Ceph module for automatically managing cache tiers

The module is designed to monitor client traffic to OSD and manage the creation of a cache-tier pool
if there is high client activity to a configured location.  

This code is more a proof-of-concept than anything production ready.  
It is also unfinished and mostly untested but seems to work as far
as checking a manually provided location over-ride and setting status internally
that should trigger the creation of a new cache.  There are some bugs actually
creating the cache and setting the internal status tables (python dicts stored in the Ceph key/value store)

Below is the online help. The module is running on our test cluster.  


```
cache add crush <crush_rule> <pool_name>                  associate a backing pool with CRUSH rule to be used for 
                                                           cache pools

cache add location <crush_rule> <location> {<int>}        associate location with CRUSH rule.  May be given as lat/
                                                           long pair or as an address string specific enough to 
                                                           lookup and identify region (state, city, zip, etc)

cache enable <crush_rule> --enable                        enable cache tier creation on demand using given CRUSH 
                                                           rule

cache list crush                                          List backing pool and cache enabled crush rule 
                                                           associations

cache list locations                                      List crush rule and location associations

cache list pools                                          List cache tier pools and status

cache list simulated                                      List manual override locations

cache no simulate location <location>                     Stop simulating high client traffic from specified 
                                                           location (specify lat,lon as listed in 'cache list 
                                                           location')
cache remove crush <crush_rule> <pool_name>               Remove crush rule association from backing pool

cache simulate location <location>                        Simulate response as if traffic were exceeding threshold(
                                                           s) for location
```
