==========================
 Adding/Removing Monitors
==========================

When you have a cluster up and running, you may add or remove monitors
from the cluster at runtime.

Adding Monitors
===============

Ceph monitors are light-weight processes that maintain a master copy of the 
cluster map. You can run a cluster with 1 monitor. We recommend at least 3 
monitors for a production cluster. Ceph monitors use PAXOS to establish 
consensus about the master cluster map, which requires a majority of
monitors running to establish a quorum for consensus about the cluster map
(e.g., 1; 3 out of 5; 4 out of 6; etc.).

Since monitors are light-weight, it is possible to run them on the same 
host as an OSD; however, we recommend running them on separate hosts. 

.. important:: A *majority* of monitors in your cluster must be able to 
   reach each other in order to establish a quorum.

Deploy your Hardware
--------------------

If you are adding a new host when adding a new monitor,  see `Hardware
Recommendations`_ for details on minimum recommendations for monitor hardware.
To add a monitor host to your cluster, first make sure you have an up-to-date
version of Linux installed (typically Ubuntu 12.04 precise). 

Add your monitor host to a rack in your cluster, connect it to the network
and ensure that it has network connectivity.

.. _Hardware Recommendations: ../../install/hardware-recommendations

Install the Required Software
-----------------------------

For manually deployed clusters, you must install Ceph packages
manually. See `Installing Debian/Ubuntu Packages`_ for details.
You should configure SSH to a user with password-less authentication
and root permissions.

.. _Installing Debian/Ubuntu Packages: ../../install/debian

For clusters deployed with Chef, create a `chef user`_, `configure
SSH keys`_, `install Ruby`_ and `install the Chef client`_ on your host. See 
`Installing Chef`_ for details.

.. _chef user: ../../install/chef#createuser
.. _configure SSH keys: ../../install/chef#genkeys
.. _install the Chef client: ../../install/chef#installchef
.. _Installing Chef: ../../install/chef
.. _install Ruby: ../../install/chef#installruby

.. _Adding a Monitor (Manual):

Adding a Monitor (Manual)
-------------------------

This procedure creates a ``ceph-mon`` data directory, retrieves the monitor map
and monitor keyring, and adds a ``ceph-mon`` daemon to your cluster.  If
this results in only two monitor daemons, you may add more monitors by
repeating this procedure until you have a sufficient number of ``ceph-mon`` 
daemons to achieve a quorum.

At this point you should define your monitor's id.  Traditionally, monitors 
have been named with single letters (``a``, ``b``, ``c``, ...), but you are 
free to define the id as you see fit.  For the purpose of this document, 
please take into account that ``{mon-id}`` should be the id you chose, 
without the ``mon.`` prefix (i.e., ``{mon-id}`` should be the ``a`` 
on ``mon.a``).

#. Create the default directory on your new monitor. :: 

	ssh {new-mon-host}
	sudo mkdir /var/lib/ceph/mon/ceph-{mon-id}

#. Create a temporary directory ``{tmp}`` to keep the files needed during 
   this process. This directory should be different from monitor's default 
   directory created in the previous step, and can be removed after all the 
   steps are taken. :: 

	mkdir {tmp}

#. Retrieve the keyring for your monitors, where ``{tmp}`` is the path to 
   the retrieved keyring, and ``{filename}`` is the name of the file containing
   the retrieved monitor key. :: 

	ceph auth get mon. -o {tmp}/{filename}

#. Retrieve the monitor map, where ``{tmp}`` is the path to 
   the retrieved monitor map, and ``{filename}`` is the name of the file 
   containing the retrieved monitor monitor map. :: 

	ceph mon getmap -o {tmp}/{filename}

#. Prepare the monitor's data directory created in the first step. You must 
   specify the path to the monitor map so that you can retrieve the 
   information about a quorum of monitors and their ``fsid``. You must also 
   specify a path to the monitor keyring:: 

	sudo ceph-mon -i {mon-id} --mkfs --monmap {tmp}/{filename} --keyring {tmp}/{filename}
	

#. Add a ``[mon.{mon-id}]`` entry for your new monitor in your ``ceph.conf`` file. ::

	[mon.c]
		host = new-mon-host
		addr = ip-addr:6789

#. Add the new monitor to the list of monitors for you cluster (runtime). This enables 
   other nodes to use this monitor during their initial startup. ::

	ceph mon add <mon-id> <ip>[:<port>]

#. Start the new monitor and it will automatically join the cluster.
   The daemon needs to know which address to bind to, either via
   ``--public-addr {ip:port}`` or by setting ``mon addr`` in the
   appropriate section of ``ceph.conf``.  For example::

	ceph-mon -i {mon-id} --public-addr {ip:port}


Removing Monitors
=================

When you remove monitors from a cluster, consider that Ceph monitors use 
PAXOS to establish consensus about the master cluster map. You must have 
a sufficient number of monitors to establish a quorum for consensus about 
the cluster map.

.. _Removing a Monitor (Manual):

Removing a Monitor (Manual)
---------------------------

This procedure removes a ``ceph-mon`` daemon from your cluster.   If this
procedure results in only two monitor daemons, you may add or remove another
monitor until you have a number of ``ceph-mon`` daemons that can achieve a 
quorum.

#. Stop the monitor. ::

	service ceph -a stop mon.{mon-id}
	
#. Remove the monitor from the cluster. ::

	ceph mon remove {mon-id}
	
#. Remove the monitor entry from ``ceph.conf``. 


Removing Monitors from an Unhealthy Cluster
-------------------------------------------

This procedure removes a ``ceph-mon`` daemon from an unhealhty cluster--i.e., 
a cluster that has placement groups that are persistently not ``active + clean``.


#. Identify a surviving monitor. :: 

	ceph mon dump

#. Navigate to a surviving monitor's ``monmap`` directory. :: 

	ssh {mon-host}
	cd /var/lib/ceph/mon/ceph-{mon-id}/monmap

#. List the directory contents and identify the last commmitted map.
   Directory contents will show a numeric list of maps. ::

	ls 	
	1  2  3  4  5  first_committed  last_committed  last_pn  latest


#. Identify the most recently committed map. ::

	sudo cat last_committed

#. Copy the most recently committed file to a temporary directory. ::

	cp /var/lib/ceph/mon/ceph-{mon-id}/monmap/{last_committed} /tmp/surviving_map
	
#. Remove the non-surviving monitors. 	For example, if you have three monitors, 
   ``mon.a``, ``mon.b``, and ``mon.c``, where only ``mon.a`` will survive, follow 
   the example below:: 

	monmaptool /tmp/surviving_map --rm {mon-id}
	#for example
	monmaptool /tmp/surviving_map --rm b
	monmaptool /tmp/surviving_map --rm c
	
#. Stop all monitors. ::

	service ceph -a stop mon
	
#. Inject the surviving map with the removed monitors into the surviving monitors. 
   For example, to inject a map into monitor ``mon.a``, follow the example below:: 

	ceph-mon -i {mon-id} --inject-monmap {map-path}
	#for example
	ceph-mon -i a --inject-monmap /etc/surviving_map


.. _Changing a Monitor's IP address:

Changing a Monitor's IP address
===============================

.. important:: Existing monitors are not supposed to change their IPs.

Ceph has strict requirements when it comes to the monitors, given that these 
are a critical component and need to maintain a quorum for the whole system 
to properly work.  For a quorum to be established, the monitors need to find 
each other.

A common misconception is that the monitors will infer the location of the other 
monitors in the cluster by reading ``ceph.conf``, but this couldn't be farthest 
from the truth.  Clients and the other daemons will in fact use ``ceph.conf`` 
to discover the monitors, but not the monitors between themselves.  If you 
refer to `Adding a Monitor (Manual)`_ you will see that you need to obtain the 
current cluster monmap when creating a new monitor, as it is one of the required 
arguments of ``ceph-mon -i {mon-id} --mkfs``.

In the following sections we will explain a bit about the monitor's 
consistency requirements, and draw a picture on why it's a bad idea to change 
the monitor's IP addresses. Finally, we will present a couple of safe ways to 
change a monitor's IP address in case of dire need.


Consistency Requirements
------------------------

The monitors will always resort to the monmap stashed on their store 
when finding the other monitors in the cluster.  This approach avoids silly 
mistakes that would be likely to happen if we were using ``ceph.conf`` 
directly, such as typos when specifying a monitor address or port that could 
break the cluster.  Above all, it confers the monitors a strict guarantee that 
the contents of their monmap are the ones that the cluster agrees on, and this 
consensus is crucial on Ceph, specially considering that the monitors are 
responsible for sharing the monmap with clients and other components in the 
cluster.

As with any other updates on the monitor, changes to the monmap always run 
through a distributed consensus algorithm called Paxos.  Each update, such 
as adding or removing a monitor, is agreed among all the monitors in the quorum, 
ensuring that all end up with the same version.  Furthermore, updates can be 
seen as incremental, meaning that the monitors will hold the latest agreed upon 
version and a reasonable set of previous versions, allowing a new monitor (or a 
monitor that was down for a considerable amount of time) to catch up with the 
current cluster state.  This is how serious map consistency is considered on Ceph.

Now for the sake of the argument, say that we were reading the other monitors' 
location (i.e., ip:port) from ``ceph.conf``.  Not only would we have to 
accordingly change the configuration file on all the nodes each time we wanted 
to add a new monitor, to reflect the new monitor, but we would be in a world of 
pain if we happened to make a mistake that could be as simple as misspelling the 
monitor's name or port on one of the nodes.  Given that the monitors' strict 
requirements for allowing other monitors in the cluster, if one of the existing 
monitors were to be unable to correctly identify another monitor, we could very 
well end up without quorum; worse, we could end up with some anomalous state in 
which most monitors recognize one other monitor as being in the cluster, while 
others don't.  The last thing one wants in a cluster is allowing room for chaos 
on a critical component such as the monitors.


Changing a Monitor's IP address (The Right Way)
-----------------------------------------------

We have established that changing the IPs on the ``ceph.conf`` is not an option 
from the monitor's point-of-view.  It's still required, of course, but only so 
that clients and the other daemons know where to contact the monitors.  From the 
monitor's perspective however, not so much.

The right way of changing a monitor's IP address comes in the form of adding a 
new monitor with the new IP address (as described in `Adding a Monitor (Manual)`_), 
prior to removing the monitor we want to get rid of.  This way will ensure that 
not only the remaining monitors know where to contact the new monitor, but it will 
also maintain availability.

For instance, lets assume there are three monitors in place, such as :: 

	[mon.a]
		host = host01
		addr = 10.0.0.1:6789
	[mon.b]
		host = host02
		addr = 10.0.0.2:6789
	[mon.c]
		host = host03
		addr = 10.0.0.3:6789

Now say that we want to change ``mon.c`` to ``host04``, which has the IP address 
``10.0.0.4``. We would follow the steps on `Adding a Monitor (Manual)`_, adding a 
new monitor ``mon.d``, and then would remove ``mon.c`` as described on 
`Removing a Monitor (Manual)`_.  One should be aware that ``mon.d`` must be 
running before removing ``mon.c``, or quorum will be broken.  Moving all three 
monitors would thus require repeating this process as many times as needed.


Changing a Monitor's IP address (The Messy Way)
-----------------------------------------------

There may come a time when the monitors must be moved to a different network, 
a different part of the datacenter or a different datacenter altogether.  While 
it is possible to do it, the process becomes a bit more hazardous.

In such a case, the solution is to generate a new monmap with updated IP 
addresses for all the monitors in the cluster, and inject the new map on each 
individual monitor.  This is not the most user-friendly approach, but we do not 
expect this to be something that needs to be done every other week.  As it is 
clearly stated on the top of this section, monitors are not supposed to change 
IP addresses.

Take the previous monitor configuration and say that we want to move all the 
monitors from the ``10.0.0.x`` range to ``10.1.0.x``, and that these networks 
are unable to communicate.  Here is what we should do in such a remote 
scenario:


#. Retrieve the monitor map, where ``{tmp}`` is the path to 
   the retrieved monitor map, and ``{filename}`` is the name of the file 
   containing the retrieved monitor monitor map. :: 

	ceph mon getmap -o {tmp}/{filename}

#. These should be the contents of the monmap. ::

	$ monmaptool --print {tmp}/{filename}
	
	monmaptool: monmap file {tmp}/{filename}
	epoch 1
	fsid 224e376d-c5fe-4504-96bb-ea6332a19e61
	last_changed 2012-12-17 02:46:41.591248
	created 2012-12-17 02:46:41.591248
	0: 10.0.0.1:6789/0 mon.a
	1: 10.0.0.2:6789/0 mon.b
	2: 10.0.0.3:6789/0 mon.c

#. Remove the existing monitors. ::

	$ monmaptool --rm a --rm b --rm c {tmp}/{filename}
	
	monmaptool: monmap file {tmp}/{filename}
	monmaptool: removing a
	monmaptool: removing b
	monmaptool: removing c
	monmaptool: writing epoch 1 to {tmp}/{filename} (0 monitors)

#. Add the new monitor locations. ::

	$ monmaptool --add a 10.1.0.1:6789 --add b 10.1.0.2:6789 --add c 10.1.0.3:6789 {tmp}/{filename}
	
	monmaptool: monmap file {tmp}/{filename}
	monmaptool: writing epoch 1 to {tmp}/{filename} (3 monitors)

#. Check new contents. ::

	$ monmaptool --print {tmp}/{filename}
	
	monmaptool: monmap file {tmp}/{filename}
	epoch 1
	fsid 224e376d-c5fe-4504-96bb-ea6332a19e61
	last_changed 2012-12-17 02:46:41.591248
	created 2012-12-17 02:46:41.591248
	0: 10.1.0.1:6789/0 mon.a
	1: 10.1.0.2:6789/0 mon.b
	2: 10.1.0.3:6789/0 mon.c

At this point, it is assumed that either the servers have physically moved to 
the new location, or that the monitor's stores were.  That said, the next step 
is to make the changed monmap available to the monitors and inject it to each 
individual monitor.

#. First, make sure to stop all your monitors.  Injection must be done while 
   the daemon is not running.

#. Inject the monmap. ::

	ceph-mon -i {mon-id} --inject-monmap {tmp}/{filename}

#. Restart the monitors.

After this step, your monitors should be able to live forever happy in their 
new homes.

