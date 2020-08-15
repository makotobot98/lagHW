# [Yarn](https://data-flair.training/blogs/hadoop-yarn-tutorial/)

- Yarn is of master-slave architecture, there are master nodes and slave nodes

## Resouce Manager(RM)
- Resource manager is the master daemon of Yarn
- two components are
  - Scheduler
  - Application manager
- in summary of RM: handle client request, start/monitor application master(AM), moniter node manager(NM), allocate resources(containers, start app masters etc)

### Scheduler
The scheduler is responsible for allocating the resources to the running application. The scheduler is pure scheduler it means that it performs no monitoring no tracking for the application and even doesn’t guarantees about restarting failed tasks either due to application failure or hardware failures.

### Application Manager
It manages running Application Masters in the cluster, i.e., it is responsible for starting application masters and for monitoring and restarting them on different nodes in case of failures.


## Node Manager (NM)
It is the slave daemon of Yarn. NM is responsible for containers monitoring their resource usage and reporting the same to the ResourceManager. Manage the user process on that machine. Yarn NodeManager also tracks the health of the node on which it is running. 
- in summary of NM: one machine(node) an Node Manager, in charge of resources on a single node, handle instructions from RM, handle instructions from AM

## Application Master (AM)
One application master runs per application. It negotiates resources from the resource manager and works with the node manager. It Manages the application life cycle.
- The AM acquires **containers** from the **RM’s Scheduler** before contacting the corresponding NMs to start the application’s individual tasks.
- track task status, restart failed tasks
- A **container** is a compute instance, it is led by a **application master**. a container encapsulates CPU, disk resouces for a task to be ran
- in summary of AM: partitioning data, request resources from RM for the application and allocate to containers acquired, monitor/failover for tasks
