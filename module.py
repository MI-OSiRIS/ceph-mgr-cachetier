import errno
import logging
import json
import subprocess
import time
import textwrap
from threading import Event, Thread

#try:
#    import queue as Queue
#except ImportError:
#    import Queue

from mgr_module import MgrModule

from geopy.geocoders import Nominatim   
from geopy.distance import geodesic

# https://pythonhosted.org/python-geoip/
# https://pypi.org/project/geopy/
# https://github.com/maxmind/GeoIP2-python
# will need to import geoip modules probably
# create some kind of test harness commands 
# so we can say something like 'test region SOMREGION' 
# and trigger whatever code path would be triggered by high traffic from that region

  # OR...could we use the geoip database to automatically decide whether clients are proximate to the storage?
        # We know the IP of storage elements, if we have some fuzzy association (regional level) then
        # the module could simply calculate that all the storage for a pool is in one region and all the 
        # storage for a given cache crush bucket is another region...

        # TODO:  
        # - Determine which clients are accessing which pools from info available to mgr
        # - Determine the crush rule for any given pool
        # - We need to know the pool to overlay with cache tier, and what crush rule to use for the tier.  
        # - To know the right crush rule we need to have an association of crush bucket to ip location

        # arguments sample
        #'cmd': 'fs subvolumegroup create '
        # 'name=vol_name,type=CephString '
        # 'name=group_name,type=CephString '
        # 'name=pool_layout,type=CephString,req=false '
        # 'name=mode,type=CephString,req=false ',

# log = logging.getLogger(__name__)

class Module(MgrModule):
    COMMANDS = [
        {
            'cmd': 'cache list pools',
            'desc': "List cache tier pools and status",
            'perm': 'r'
        },
        {
            'cmd': 'cache list crush',
            'desc': 'List backing pool and cache enabled crush rule associations',
            'perm': 'r'
        },
        {
            'cmd': 'cache list simulated',
            'desc': 'List manual override locations',
            'perm': 'r'
        },
        {
            'cmd': 'cache list locations',
            'desc': 'List crush rule and location associations',
            'perm': 'r'
        },
        {
            'cmd': 'cache add crush '
                   'name=crush_rule,type=CephString '
                   'name=pool_name,type=CephString ',
            'desc': "associate a backing pool with CRUSH rule to be used for cache pools",
            'perm': 'rw'
        },
        {
            'cmd': 'cache remove crush '
                   'name=crush_rule,type=CephString '
                   'name=pool_name,type=CephString ',
            'desc': "Remove crush rule association from backing pool",
            'perm': 'rw'
        },
        {
            'cmd': 'cache enable '
                   'name=crush_rule,type=CephString '
                   'name=enable,type=CephBool ',
            'desc': "enable cache tier creation on demand using given CRUSH rule",
            'perm': 'rw'
        },
        {
            'cmd': 'cache add location '
                   'name=crush_rule,type=CephString '
                   'name=location,type=CephString '
                   'name=proximity,type=CephInt,req=false ',
            'desc': "associate location with CRUSH rule.  May be given as lat/long pair or as an address string specific enough to lookup and identify region (state, city, zip, etc)",
            'perm': 'rw'
        },
        {
            'cmd': 'cache simulate location '
                   'name=location,type=CephString ',
            'desc': "Simulate response as if traffic were exceeding threshold(s) for location",
            'perm': 'rw'
        },
        {
            'cmd': 'cache no simulate location '
                   'name=location,type=CephString ',
            'desc': "Stop simulating high client traffic from specified location (specify lat,lon as listed in 'cache list location')",
            'perm': 'rw'
        },

    ]

    MODULE_OPTIONS = [
        {
            'name': 'traffic_threshold_bytes',
            'desc': 'avg bytes/s of traffic to create or remove cache tiers',
            'type': 'int',
            'runtime': True
        },
        {
            'name': 'traffic_threshold_ratio',
            'desc': 'ratio of traffic from geoip block vs other traffic to create or remove cache tiers',
            'type': 'int',
            'runtime': True
        },
         {
            'name': 'default_cache_size',
            'type': 'int',
            'default': 1024,
            'desc': 'default max size in MB for for cache pools (converted to bytes to set target_max_bytes).  Default 1024MB',
            'runtime': True
        },
        {
            'name': 'default_cache_objects',
            'type': 'int',
            'default': 0,
            'desc': 'default max objects for cache pools (sets target_max_objects).  Default is disabled (0)',
            'runtime': True
        },
        {
            'name': 'proximity',
            'desc': 'Max client distance in miles from cache tier location which will trigger new tier creation',
            'type': 'int',
            'default': 100,
            'runtime': True
        },
        {
            'name': 'cooldown_duration',
            'desc': 'how long to leave cache tier in place after thresholds are no longer met (seconds), default 1800 (30 mins), 0 to disable automatic removal',
            'type': 'int',
            'default': 10,
             # 'default': 1800,
            'runtime': True
        },
        {
            'name': 'min_size',
            'type': 'int',
            'default': 2,
            'desc': 'default min_size for new cache pools',
            'runtime': True
        },
        {
            'name': 'size',
            'type': 'int',
            'default': 3,
            'desc': 'default size (replicas) for new cache pools',
            'runtime': True
        },
        {
            'name': 'pg_num',
            'type': 'int',
            'default': 4,
            'desc': 'default pg_num for new cache pools',
            'runtime': True
        },
        {
            'name': 'suffix',
            'type': 'str',
            'default': '.cache',
            'desc': 'suffix appended to new cache tier pools (name will be backing pool name + suffix)',
            'runtime': True
        },

    ]

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        self.geolocator = Nominatim(user_agent="osiris-ceph-mgr-cachetier")
        self.interval = 60
        self.event = Event()
        self.workers = dict()
        self.suffix = self.get_module_option('suffix')
        self.cooldown = self.get_module_option('cooldown_duration')
        self.proximity = self.get_module_option('proximity')
        self.pg_num = self.get_module_option('pg_num')
        self.run = True
        # self.tasks = queue.Queue(maxsize=100)
        # queue for tasks
        # self.queue = Queue.Queue(maxsize=100)

        # location = geolocator.reverse("52.509669, 13.376294")
        # location = geolocator.geocode("175 5th Avenue NYC")
        # might need this...
        # self._background_jobs = Queue.Queue()

    def serve(self):
        self.log.info('Starting cachetier module')
        while self.run:
            self.poll_traffic()
            self.manage_cache()
            self.log.info("Finished traffic poll and cache management loop, sleeping for {} seconds".format(self.interval))
            self.event.wait(self.interval)

    def shutdown(self):
        self.log.info('Stopping cachetier module')
        self.run = False
        self.event.set()
            
    def handle_command(self, inbuf, cmd):
        handler_name = "_cmd_" + cmd['prefix'].replace(" ", "_")
        try:
            handler = getattr(self, handler_name)
        except AttributeError:
            return -errno.EINVAL, "", "Unknown command"

        return handler(inbuf, cmd)

    # not built into MgrModule for some reason...
    def get_pretty_footer(self, width):
        # dash line
        ret = '+'
        ret += '-' * (width - 1) + '+'
        ret += '\n'
        return ret

    def _cmd_cache_list_crush(self,inbuf,cmd):
        stored_pools = self.fetch('cache_assoc')
        ret = ''
        ret += self.get_pretty_header(('Pool', 'Cache Targets (crush rules)'), 80)
        for pool in stored_pools:
            for crush in stored_pools[pool]:
                ret += self.get_pretty_row((pool,crush), 80) + '\n'

        ret += self.get_pretty_footer(80)
        return (0, '', ret)
        # ({'backing_pool': pool, 'cache_pool': "{}.{}".format(pool, self.suffix) , 'state': 'active', 'timestamp': time.time() })

    def _cmd_cache_list_locations(self,inbuf,cmd):
        stored_loc = self.fetch('loc_assoc')
        ret = ''
        ret += self.get_pretty_header(("Crush - Lat/Lon - Proximity", "Location Desc"), 80)

        for crush_rule in stored_loc:
            for ldata in stored_loc[crush_rule]:
                lat = ldata[0]
                lon = ldata[1]
                prox = ldata[2]
                latlon = "{},{}".format(lat,lon)
                location = self.geolocator.reverse((lat,lon)).address.encode('utf-8')
                cell1 = (crush_rule, latlon, '{} Miles'.format(prox))
                cell2 = textwrap.wrap(location, width=40)
                for idx in range(0, len(cell2)):
                    if idx < len(cell1):
                        row_elem = cell1[idx]
                    else:
                        row_elem = ''
                    ret += self.get_pretty_row((row_elem, cell2[idx]), 80) + '\n'
        ret += self.get_pretty_footer(80)
        return (0, '', ret)
                
    def _cmd_cache_list_pools(self,inbuf,cmd):
        stored_active = self.fetch('cache_active')
        ret = ''
        ret += self.get_pretty_header(('Pool', 'Cache Pool', 'Crush', 'Status', 'Created'), 80)

        for crush_rule in stored_active:
            ret = ''
            for cache_info in stored_active[crush_rule]:
                time = time.localtime(cache_info['timestamp']).strftime("%m-%d-%y %H:%M:%S %Z")
                row_elems = (cache_info['backing_pool'], cache_info['cache_pool'], crush_rule, cache_info['state'], time)
                ret += self.get_pretty_row(row_elems, 80) + '\n'
        ret += self.get_pretty_footer(80)

        return (0, '', ret)

    def _cmd_cache_list_simulated(self,inbuf,cmd):
        stored_override = self.fetch('loc_override')
        ret = ''
        ret += self.get_pretty_header(('Lat/Lon', 'Identifier'), 80)
        for loc in stored_override:
            latlon = "{},{}".format(loc[0],loc[1])
            # ret += latlon
            # if we don't explicitely encode utf-8 here we get an 'ordinal not in range' exception printing out the address
            # UnicodeEncodeError: 'ascii' codec can't encode character u'\xa0' in position 41: ordinal not in range(128)
            location =  self.geolocator.reverse(loc).address.encode('utf-8')
            location = textwrap.wrap(location, width=38)
            for location_line in location:
                elems = (latlon,location_line)
                # only print this on first line
                latlon = ''
                ret += self.get_pretty_row(elems, 80) + '\n'
            ret += self.get_pretty_footer(80)

        return (0, '', ret)

    # return 3-tuple result code, output buffer, informative string
    def _cmd_cache_add_crush(self,inbuf,cmd):

        # check if already set before fetching data from cluster
        stored_pools = self.fetch('cache_assoc')

        if cmd['pool_name'] in stored_pools:
            if cmd['crush_rule'] in stored_pools[cmd['pool_name']]:
                return  (0,"","Association of {} with crush root {} already set".format(cmd['pool_name'],cmd['crush_rule']))

        # tree = self.get('osd_map_tree')
        osdmap = self.get_osdmap()
        crushmap = osdmap.get_crush().dump()

        for pool_id, pool in osdmap.get_pools().items():
            if pool['pool_name'] == cmd['pool_name']:
                # pool exists
                for rule in crushmap['rules']:
                    if rule['rule_name'] == cmd['crush_rule']:
                        stored_pools.setdefault(cmd['pool_name'],[]).append(cmd['crush_rule'])
                        self.set_store('cache_assoc', json.dumps(stored_pools))
                        return (0,"","Associated {} with crush rule {} for cache overlays".format(cmd['pool_name'],cmd['crush_rule']))
        return (-errno.EINVAL, "", "Pool or crush rule does not exist")

    def _cmd_cache_remove_crush(self,inbuf,cmd):
        stored_pools = self.fetch('cache_assoc')
        if cmd['pool_name'] in stored_pools:
            if cmd['crush_rule'] in stored_pools[cmd['pool_name']]:
                stored_pools[cmd['pool_name']].remove(cmd['crush_rule'])
                self.store('cache_assoc', stored_pools)
                return  (0,"","Association of {} with crush root {} removed".format(cmd['pool_name'],cmd['crush_rule']))


    def _cmd_cache_add_location(self,inbuf,cmd):
        stored_loc = self.fetch('loc_assoc')

        location_geocode = self.geolocator.geocode(cmd['location'])

        if location_geocode == None:
            return self.hande_err('geocode', location=cmd['location'])

        if 'proximity' in cmd:
            setprox = cmd['proximity']
        else:
            setprox = self.proximity

        if cmd['crush_rule'] in stored_loc:
            for lat,lon,prox in stored_loc[cmd['crush_rule']]:
                if location_geocode.latitude == lat and location_geocode.longitude == lon:
                    if prox == setprox:
                        return(0,"", "Location {},{} with proximity {} miles already associated with crush root {} (argument provided as {})"
                        .format(location_geocode.latitude, location_geocode.longitude, prox, cmd['crush_rule'], cmd['location']))
                    else:
                        stored_loc[cmd['crush_rule']].remove([lat,lon,prox])
                        stored_loc[cmd['crush_rule']].append([lat,lon,setprox])
                        self.store('loc_assoc', stored_loc)
                        return(0,"","Location already associated - updated location proximity to {} miles".format(setprox))

        osdmap = self.get_osdmap()
        crushmap = osdmap.get_crush().dump()

        for rule in crushmap['rules']:
            self.log.info("_cmd_cache_associate_location: crush rule {} being compared to user specified rule".format(rule['rule_name']))
            # make sure it exists
            if rule['rule_name'] == cmd['crush_rule']:
                stored_loc[cmd['crush_rule']] = [[location_geocode.latitude, location_geocode.longitude, setprox]]
                self.store('loc_assoc', stored_loc)
                return (0,"", "Location {},{} now associated with crush rule") 
                        
                        # {}".format(location_geocode.latitude, location_geocode.longitude, rule['rule_name']))

        return (-errno.EINVAL, '', "Crush rule {} not found".format(cmd['crush_rule'])) 

    def _cmd_cache_enable(self,inbuf,cmd):
        # verify there is a location association
        stored_loc = self.fetch('loc_assoc')
        if cmd['crush_rule'] in stored_loc:
            stored_enable = self.fetch('loc_enable')
            stored_enable[cmd['crush_rule']] = cmd['enable']
            self.store('loc_enable', stored_enable)
            return(0,"","Enabled cache creation for crush root {}".format(cmd['crush_rule']))
        return(-errno.EINVAL, '',"Crush root {} not found".format(cmd['crush_rule'])) 

    # set overide in datastore, will be picked up by traffic poller and applied
    def _cmd_cache_simulate_location(self,inbuf,cmd):
        return self.cache_simulate_location(cmd['location'], enable=True)

    def _cmd_cache_no_simulate_location(self,inbuf,cmd):
        return self.cache_simulate_location(cmd['location'], enable=False)

     # set overide in datastore, will be picked up by traffic poller and applied
    def cache_simulate_location(self,location, enable=True):
        stored_override = self.fetch('loc_override', default = 'list')

        location_geocode = self.geolocator.geocode(location)
        if location_geocode == None:
            return self.hande_err('geocode', location=location)

        if enable == True:
            if [location_geocode.latitude, location_geocode.longitude] not in stored_override:
                stored_override.append([location_geocode.latitude, location_geocode.longitude])
            rmsg = "Simulating high traffic threshold for"
        elif [location_geocode.latitude, location_geocode.longitude] in stored_override:
                stored_override.remove([location_geocode.latitude, location_geocode.longitude])
                rmsg = "Stopped simulating traffic for"
        else:
            return(-errno.EINVAL, '', 'Location not found in simulated locations')

        self.store('loc_override',stored_override)

        return (0, "", "{} {},{} ({})".format(rmsg,location_geocode.latitude, location_geocode.latitude, location_geocode.address.encode('utf-8')))

    def err_s(self, msg, pool=None,state=None,location=None):
        errmap = dict()
        errmap['geocode'] = (-errno.EINVAL, "", "Location {} not found by geocode lookup".format(location))
        errmap['poolstate'] = "Setting cache pool {} state {} failed".format(pool, state)

        return errmap[msg]
        
    def poll_traffic(self):
 
        self.log.info("Polling traffic")

        #stored_loc
        # crush -> location list (lat,lon,prox) tuples

        # stored_pools
        # pools -> crush rule list

        #stored_loc = self.fetch('loc_assoc')
        # stored_assoc = self.fetch('cache_assoc')
        stored_loc = self.fetch('loc_assoc')
        stored_override = self.fetch('loc_override', default ='list')
        stored_active = self.fetch('cache_active')
        stored_pools = self.fetch('cache_assoc')

        self.log.info("poll_traffic: Retrieved stored cache status: {}".format(stored_active))

        # we only care about crush rules that have pool associations

        for pool in stored_pools:
            for crush in stored_pools[pool]:
                self.log.info("poll_traffic: pool {}: associated cache crush rule {} being checked for any location association ".format(pool, crush))
                if crush in stored_loc:
                    for loclist in stored_loc[crush]:
                        lat = loclist[0]
                        lon = loclist[1]
                        prox = loclist[2]
                        self.log.info("poll_traffic: pool {}: cache crush rule {} has association with location {},{} ".format(pool, crush, lat,lon))

                        #  pseudo code block for doing this with actual network data
                        #  query:  traffic to OSD which are included in a crush rule associated with a location proximity
                        #  filter: sum of all traffic by location exceeding threshold
                        #  if location exceeds configured threshold add coordinates to network_locations list
                        #  combine that list with user over-ride locations 
                        #  iterate through list and trigger cache startup state if a crush rule -> location association exists within proximity
                        #  any crush rule -> location associations not in list need to be marked as teardown if past timeout

                        # ... but right now there is no real network data to include
                        network_locations = []
                        trigger_locations = stored_override + network_locations

                        # mark active caches for teardown if cooldown exceeded (will be over-ridden by location trigger check)
                        if crush in stored_active:
                            for cache_info in stored_active[crush]:
                                if cache_info['state'] == 'active' and (time.time() - cache_info['timestamp'] > self.cooldown):
                                    self.log.info("poll_trafic: cache pool {}: cooldown expired, marking for teardown".format(cache_info['cache_pool']))
                                    cache_info['state'] = 'teardown'


                        # check list of trigger locations and activate new caches if necessary
                        for location in trigger_locations:
                            distance = geodesic((lat,lon), location)

                            # is the overide location within specified proximity to any stored location/crush association?
                            if distance <= prox:
                                self.log.info("poll_traffic: location {},{} triggered cache activation for pool {} using crush rule {}".format(lat,lon, pool, crush))
                                if crush in stored_active:
                                    for cache_info in stored_active[crush]:
                                        if cache_info['state'] == 'active':
                                            self.log.info("poll_traffic: Pool {} is already active".format(cache_info['cache_pool']))
                                            # reset timestamp used for cooldown 
                                            cache_info['timestamp'] = time.time()
                                            continue
                                        else:
                                            self.log.info("poll_traffic: Pool {} changed from state {} to state {}".format(cache_info['cache_pool'], cache_info['state'], 'startup'))
                                            cache_info['timestamp'] = time.time()
                                            cache_info['state'] = 'startup'
                                else:
                                    self.log.info("poll_traffic: Pool {} in crush {} added to stored status object with state 'startup'".format("{}{}".format(pool, self.suffix), crush))
                                    stored_active[crush] = [{'backing_pool': pool, 'cache_pool': "{}{}".format(pool, self.suffix) , 'state': 'startup', 'timestamp': time.time() }]

        # the only thing we change here is the cache active status
        self.log.info("poll_traffic: storing new changes to cache status")
        self.store('cache_active', stored_active)


# ' while running on mgr.um-testmon01: string indices must be integers
# 2019-11-19 13:35:36.540 7fbc79c05700 -1 cachetier.serve:
# 2019-11-19 13:35:36.540 7fbc79c05700 -1 Traceback (most recent call last):
#   File "/usr/share/ceph/mgr/cachetier/module.py", line 202, in serve
#     self.poll_traffic()
#   File "/usr/share/ceph/mgr/cachetier/module.py", line 454, in poll_traffic
#     if cache_info['state'] == 'active' and (time.time() - cache_info['timestamp'] > self.cooldown):
# TypeError: string indices must be integers

    def manage_cache(self):
        self.log.info("manage_cache: starting loop through status object")
        
        stored_active = self.fetch('cache_active')

        for crush_rule in stored_active:
            self.log.info("Checking cache pools for rule {}".format(crush_rule))
            for cache_info in stored_active[crush_rule]:
                # lindex = stored_active[crush_rule].index(cache_info)
                self.log.info("Pool {}:  State is marked {}".format(cache_info['cache_pool'],cache_info['state']))
                # cache is drained and ready for teardown
                if cache_info['state'] == 'empty':
                    self.log.info("Pool {}:  triggering removal".format(cache_info['cache_pool']))

                    if self.remove_cache(stored_active['cache_pool'], stored_active['backing_pool']):
                        stored_active[crush_rule].remove(cache_info)
                    else:
                        # pool was not empty, reset the process
                        self.log.info("Pool {}:  not empty, resetting state to draining".format(cache_info['cache_pool']))
                        cache_info['state'] = 'draining'
                
                # cache is marked draining - check if empty
                if cache_info['state'] == 'draining':
                    self.log.info("Pool {}:  checking for drain thread active".format(cache_info['cache_pool']))
                    if cache_info['cache_pool'] in self.workers:
                        thread = self.workers[cache_info['cache_pool']]
                        if thread.is_alive():
                            return
                        else:
                            self.log.info("Pool {}:  drain thread complete, marking empty".format(cache_info['cache_pool']))
                            cache_info['state'] = 'empty'
                            self.workers[cache_info['cache_pool']].remove()

                    # no thread is working on draining it, set state back to 'teardown' and trigger flush again
                    else:
                        self.log.info("Pool {}:  no thread is working on draining, resetting state to teardown".format(cache_info['cache_pool']))
                        cache_info['state'] = 'teardown'
            
                
                # cache is no longer required and should begin draining and teardown
                if cache_info['state'] == 'teardown': 
                    self.log.info("Pool {}: starting teardown/drain thread".format(cache_info['cache_pool']))
                    worker = Thread(target=self.flush_cache, args=(cache_info['cache_pool']))
                    worker.setDaemon(True)
                    worker.start()
                    # wait for thread to initialize and begin flushing
                    while worker.is_alive() and not self.event.wait():
                        self.workers[cache_info['cache_pool']] = worker
                        cache_info['state'] = 'draining'
                    
                    if cache_info['state'] != 'draining':
                        self.log.error(self.err_s('poolstate', pool=cache_info['cache_pool'], state='draining'))

                # cache needs to started up
                if cache_info['state'] == 'startup':
                    self.log.info("Pool {}: creating cache pool and setting state active".format(cache_info['cache_pool']))
                    # we may eventually need to incorporate options for min_size, max_size, etcs
                    if self.create_cache(cache_pool=cache_info['cache_pool'], backing_pool=cache_info['backing_pool'], crush_rule=crush_rule):
                        cache_info['state'] = 'active'
                else:
                    self.log.error(self.err_s('poolstate', pool=cache_info['cache_pool'], state='active'))

                # stored_active[crush_rule][lindex] = cache_info

        # stored_active.setdefault(crush_rule, ()).append({'backing_pool': backing_pool, 'cache_pool': cache_pool, 'state': 'active' })
        self.log.info("Storing current cache_active status {}".format(stored_active))
        self.store('cache_active', stored_active)

    # at this point I'm not quite sure how to check if a cache pool is actually 
    # configured as an overlay so the best we can do is check that it exists 
    # if ecprofile is provided then the pool type argument automatically changes to erasure
    # if pg_num is not provided the module global default setting is used
    def create_cache(self,cache_pool,backing_pool,crush_rule, pg_num=None,ecprofile=None, size=None, min_size=None, max_bytes=None, max_objects=None):
        self.log.info("create_cache: pool: {}, cache: {}, crush: {}".format(backing_pool,cache_pool, crush_rule))

        if size == None:
            size = self.get_module_option('size')

        if min_size == None:
            min_size = self.get_module_option('min_size')

        if pg_num == None:
            pg_num = self.get_module_option('pg_num')

        if max_bytes == None:
            max_bytes = self.get_module_option('default_cache_size')

        if max_objects == None:
            max_objects = self.get_module_option('default_cache_objects')

        writeback = 'writeback'

        pool_cmd = { "prefix": "osd pool create",
                "pool": cache_pool,
                "pg_num": pg_num,
                "pgp_num": pg_num,
                "pool_type":  'replicated',
                "rule": crush_rule,
                "size": size
        }

        if ecprofile:
            pool_cmd['erasure_code_profile'] = ecprofile
            pool_cmd['pool_type'] = 'erasure'

        pool_min_size = {
            "prefix": "osd pool set",
            "pool": cache_pool,
            "var": "min_size",
            "val": str(min_size)
        }

        tier_add = { 
            "prefix": "osd tier add",
            "pool" : backing_pool,
            "tierpool": cache_pool
        }

        cache_mode = {
            "prefix": "osd tier cache-mode",
            "pool": cache_pool,
            "mode": writeback,
        }

        set_overlay = {
            "prefix": "osd tier set-overlay",
            "pool": backing_pool,
            "overlaypool": cache_pool
        }

        hit_set = {
            "prefix": "osd pool set",
            "pool": cache_pool,
            "var": "hit_set_type",
            "val": "bloom"
        }

        max_bytes_cmd = {
            "prefix": "osd pool set",
            "pool": cache_pool,
            "var": "target_max_bytes",
            "val":  str(max_bytes)
        }

        run_cmds = (pool_cmd, pool_min_size, tier_add, cache_mode, set_overlay, hit_set, max_bytes_cmd)

        if max_objects > 0:
            max_objects_cmd = {
                "prefix": "osd pool set",
                "pool": cache_pool,
                "var": "target_max_objects",
                "val":  str(max_objects)
            }

            run_cmds.append(max_objects_cmd)

        rcode = 0
        i = 0
        while rcode == 0 and i < len(run_cmds):
            self.log.info("Running command: {}".format(run_cmds[i]))
            rcode, stdout, errstr = self.mon_command(run_cmds[i])
            i += 1

        if rcode != 0:
            self.log.error("Pool creation failed for cache pool {}: {}".format(cache_pool, errstr))
            return False

        return True



#  "name=var,type=CephChoices,strings=size|min_size|pg_num|pgp_num|pgp_num_actual|crush_rule|hashpspool|nodelete|nopgchange|nosizechange|write_fadvise_dontneed|noscrub|nodeep-scrub|hit_set_type|hit_set_period|hit_set_count|hit_set_fpp|use_gmt_hitset|target_max_bytes|target_max_objects|cache_target_dirty_ratio|cache_target_dirty_high_ratio|cache_target_full_ratio|cache_min_flush_age|cache_min_evict_age|min_read_recency_for_promote|min_write_recency_for_promote|fast_read|hit_set_grade_decay_rate|hit_set_search_last_n|scrub_min_interval|scrub_max_interval|deep_scrub_interval|recovery_priority|recovery_op_priority|scrub_priority|compression_mode|compression_algorithm|compression_required_ratio|compression_max_blob_size|compression_min_blob_size|csum_type|csum_min_block|csum_max_block|allow_ec_overwrites|fingerprint_algorithm|pg_autoscale_mode|pg_autoscale_bias|pg_num_min|target_size_bytes|target_size_ratio " \
#      "name=val,type=CephString " \
#      "name=yes_i_really_mean_it,type=CephBool,req=false", \
#      "set pool parameter <var> to <val>", "osd", "rw")
#ceph osd pool create cou.VAI.fs.cache 512 512 replicated vai-cache
#ceph osd tier add cou.VAI.fs cou.VAI.fs.cache
#ceph osd tier cache-mode cou.VAI.fs.cache writeback
#ceph osd tier set-overlay cou.VAI.fs cou.VAI.fs.cache
#ceph osd pool set cou.VAI.fs.cache hit_set_type bloom

    # run in background thread to flush cache
    def flush_cache(self,pool_name):
        self.log.info("flush_cache: pool {}".format(pool_name))
        return 

        rcode, stdout, errstr = self.mon_command({
                        "prefix": "osd tier cache-mode",
                        "pool": pool_name,
                        "mode": "forward",
                        "yes_i_really_mean_it": True
                    })
        if rcode != 0:
            self.log.error("Error setting {} to cache-mode forward for flushing: {}".format(pool_name, errstr))
            return False
        
        self.event.set()

        # wait for flush to finish
        rcode = subprocess.call(['rados', '-p',  pool_name, 'cache-flush-evict-all'])
        if rcode != 0: 
            self.log.error("Cache flush evict all failed for pool {}".format(pool_name))
            return False

        return

    def remove_cache(self,cache_pool, backing_pool):
        self.log.info("remove_cache: backing pool {}, cache pool {}".format(backing_pool, cache_pool))
        return 

        # verify really empty
        try:
            pool_contents = subprocess.check_output(['rados', '-p', cache_pool, 'ls'])
        except CalledProcessError as cpe:
            self.log.error("remove_cache: pool {} error checking contents {}: {}".format(cache_pool,cpe.returncode, cpe.output))
            return False
        if len(pool_contents) > 0:
            self.log.error("remove_cache:  pool {} is not empty, not removing cache".format(pool_name))
            return False

        rcode, stdout, errstr = self.mon_command({
                         "prefix": "osd tier rm-overlay",
                         "pool": backing_pool
                         })

        if rcode != 0:
            self.log.error("Error removing cache overlay from pool {}: {}".format(backing_pool, errstr))
            return False

        rcode, stdout, errstr = self.mon_command({
                         "prefix": "osd tier remove",
                         "pool": backing_pool,
                         "tierpool":  cache_pool
                         })

        if rcode != 0:
            self.log.error("Error removing cache tier {} from pool {}: {}".format(cache_pool, backing_pool, errstr))
            return False

        self.log.info("Removed cache tier {} from pool {}".format(cache_pool, backing_pool))
        return True

    # fetch json dicts or lists from datastore or initialize for use if not yet stored
    def fetch(self,storekey, default = 'dict'):
        stored = self.get_store(storekey)
        if stored == None:
            if default == 'list':
                stored = list() 
            elif default == 'dict':
                stored = dict()
        else:
            stored = json.loads(stored)

        return stored

    # store dict into datastore 
    def store(self,storekey,data):
        self.set_store(storekey, json.dumps(data))

#
#
#
#                cr_name = crush.get_rule_by_id(pool['crush_rule'])['rule_name']
#                root_id = int(crush.get_rule_root(cr_name))
#            pool_root[pool_id] = root_id

        # { poolid: {pool keys} }
        # self.log.error("HERE IS AN OSDMAP EXAMPLE: \n\n {}".format(osdmap))
        
        #self.log.error("HERE IS A CRUSHMAP EXAMPLE: \n\n {}".format(crushmap))

        # crush_exists = crushmap.get_item_name(cmd['crush_rule'])
        #self.log.info('Crush received: {}'.format(cmd['crush_rule']))
        # self.log.info('Crush looked up: {}'.format(crush_exists))
        
        # self.log.info('Crush name: {}'.format(crush_exists))
        #cmd['crush_rule']
        #cmd['pool_name']


# send command

# result = CommandResult("")
#                     self.send_command(result, "mon", "", json.dumps({
#                         "prefix": "osd pool set",
#                         "format": "json",
#                         "pool": pool_name,
#                         'var': 'size',
#                         "val": str(num_rep),
#                     }), "")
#                     r, outb, outs = result.wait()

        # sample pool object from osdmap.get_pools()
        # {128L: {'cache_target_full_ratio_micro': 800000L, 'fast_read': False, 'stripe_width': 0L, 'flags_names': 'hashpspool', 'tier_of': -1L, 'hit_set_grade_decay_rate': 0L, 'pg_placement_num': 8L, 'use_gmt_hitset': True, 'last_force_op_resend_preluminous': '0', 'create_time': '2019-09-04 16:25:15.560679', 'quota_max_bytes': 0L, 'erasure_code_profile': '', 'pg_autoscale_mode': 'on', 'snap_seq': 0L, 'expected_num_objects': 0L, 'size': 3L, 'pg_num_pending': 8L, 'auid': 0L, 'cache_min_flush_age': 0L, 'hit_set_period': 0L, 'min_read_recency_for_promote': 0L, 'target_max_objects': 0L, 'pg_placement_num_target': 8L, 'pg_num': 8L, 'type': 1L, 'grade_table': [], 'pool_name': 'cou.Jetscape.rados', 'cache_min_evict_age': 0L, 'snap_mode': 'selfmanaged', 'pg_num_target': 8L, 'cache_mode': 'none', 'min_size': 2L, 'cache_target_dirty_high_ratio_micro': 600000L, 'object_hash': 2L, 'last_pg_merge_meta': {'ready_epoch': 0L, 'source_version': "0'0", 'source_pgid': '0.0', 'last_epoch_clean': 0L, 'target_version': "0'0", 'last_epoch_started': 0L}, 'write_tier': -1L, 'cache_target_dirty_ratio_micro': 400000L, 'pool': 128L, 'removed_snaps': '[]', 'crush_rule': 0L, 'tiers': [], 'hit_set_params': {'type': 'none'}, 'last_force_op_resend': '0', 'pool_snaps': [], 'quota_max_objects': 0L, 'last_force_op_resend_prenautilus': '0', 'application_metadata': {'rados': {}}, 'options': {}, 'hit_set_count': 0L, 'flags': 1L, 'target_max_bytes': 0L, 'snap_epoch': 0L, 'hit_set_search_last_n': 0L, 'last_change': '7056', 'min_write_recency_for_promote': 0L, 'read_tier': -1L}

# sample crushmap dump get_crush()
# 'rules': [{'min_size': 1L, 'rule_name': 'replicated_ruleset', 'steps': [{'item_name': 'default', 'item': -1L, 'op': 'take'}, {'num': 0L, 'type': 'host', 'op': 'chooseleaf_firstn'}, {'op': 'emit'}], 'ruleset': 0L, 'type': 1L, 'rule_id': 0L, 'max_size': 10L}, {'min_size': 1L, 'rule_name': 'ec-21-members', 'steps': [{'num': 5L, 'op': 'set_chooseleaf_tries'}, {'num': 100L, 'op': 'set_choose_tries'}, {'item_name': 'um', 'item': -5L, 'op': 'take'}, {'num': 1L, 'type': 'host', 'op': 'chooseleaf_firstn'}, {'op': 'emit'}, {'item_name': 'msu', 'item': -13L, 'op': 'take'}, {'num': 1L, 'type': 'host', 'op': 'chooseleaf_firstn'}, {'op': 'emit'}, {'item_name': 'wsu', 'item': -9L, 'op': 'take'}, {'num': 1L, 'type': 'host', 'op': 'chooseleaf_firstn'}, {'op': 'emit'}], 'ruleset': 2L, 'type': 3L, 'rule_id': 2L, 'max_size': 3L}], '

# buckets': [{'hash': 'rjenkins1', 'name': 'default', 'weight': 4494L, 'type_id': 14L, 'alg': 'straw2', 'type_name': 'root', 'items': [{'id': -5L, 'weight': 1926L, 'pos': 0L}, {'id': -9L, 'weight': 1284L, 'pos': 1L}, {'id': -13L, 'weight': 1284L, 'pos': 2L}], 'id': -1L}, {'hash': 'rjenkins1', 'name': 'um-stor-test01', 'weight': 1926L, 'type_id': 7L, 'alg': 'straw2', 'type_name': 'host', 'items': [{'id': 0L, 'weight': 642L, 'pos': 0L}, {'id': 1L, 'weight': 642L, 'pos': 1L}, {'id': 6L, 'weight': 642L, 'pos': 2L}], 'id': -2L}, {'hash': 'rjenkins1', 'name': 'um-20W-A', 'weight': 1926L, 'type_id': 8L, 'alg': 'straw2', 'type_name': 'rack', 'items': [{'id': -2L, 'weight': 1926L, 'pos': 0L}], 'id': -3L}, {'hash': 'rjenkins1', 'name': 'um-macc', 'weight': 1926L, 'type_id': 11L, 'alg': 'straw2', 'type_name': 'building', 'items': [{'id': -3L, 'weight': 1926L, 'pos': 0L}], 'id': -4L}, {'hash': 'rjenkins1', 'name': 'um', 'weight': 1926L, 'type_id': 12L, 'alg': 'straw2', 'type_name': 'member', 'items': [{'id': -4L, 'weight': 1926L, 'pos': 0L}], 'id': -5L}, {'hash': 'rjenkins1', 'name': 'wsu-stor-test01', 'weight': 1284L, 'type_id': 7L, 'alg': 'straw2', 'type_name': 'host', 'items': [{'id': 10L, 'weight': 642L, 'pos': 0L}, {'id': 2L, 'weight': 642L, 'pos': 1L}], 'id': -6L}, {'hash': 'rjenkins1', 'name': 'wsu-304', 'weight': 1284L, 'type_id': 8L, 'alg': 'straw2', 'type_name': 'rack', 'items': [{'id': -6L, 'weight': 1284L, 'pos': 0L}], 'id': -7L}, {'hash
