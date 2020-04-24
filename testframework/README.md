# TestFramework

TODO: find a name for this project

Beside building a distributed, consistent messaging queue we want to tackle another large problem: building a new Human friendly test framework able to test distributed systems in Kubernetes.

## Requirements:

The solution should 
* be cloud native and support Kubernetes. 
* be open source
* be extensible (change it/add new components)
* preferable in Go

## Defining test scenarios
* It should be human friendly, meaning the tests scenarious should be defined in a UI like flows
* Components (that can be linked/drag&drop in the UI) should be reusable
These requirements should be similar with the ETL frameworks (Airflow,Nifi) or CI Pipelines (TODO research)
* Permutations/Combinatorics of scenarios: example, be able to have 2 compoenents part of a test scenario component A has a series of parameters and 2 sets of params defined. Component B has 4 sets of parameters. The framework should run all pairs of them: first test A1+B1, 2nd A1+B2, A1+B3,A1+B4, A2+B1....
* a test scenario should accept multiple instances of the same module
* have the concept of a SUCCESS/FAILED scenario
* partial Scenarios (meaning we can group 4-5 components and reuse them in 6 scenarios, with the same flows/settings)

## Running tests
The running engine
The engine should:
* be able to run in K8s without great effort
* allow parallel tests
* automatically detect free resources or wait for them to be free


## Components
We should be able to define our own components, preferable in Go. Components should accept Parameters and be kept in a version control system, as a code.

Preferable it should:
* accept Go components or be agnostic on the components language (ex: Nifi is no good since it requires components to be written in Java), if it accepts Binaries or a simple scripting language where we can call our own scripts/binaries would be perfect
* be reusable and accept config parameters
* have prebuilt components of RETRY, WAIT/SLEEP ....
 
 Examples of components (that we will build ourselves)
* create/delete a K8S namespace
* start a Redis cluster and return its address/port
* start a dejaQ broker/producer/consumer
* pause/kill a container
* add network lag/lost packages (mess with the network)
* read prometheus metrics
* parse junitXML or other popular test results



## Solutions 

I don't think such a framework exists so my idea is to use an already built system , modify it and write our own components. 

## take an existing UI from ETL job system

We can take an open source solution that allows defining flows (but in our case they will be test scenarios).
Examples (need to research more): 
https://nifi.apache.org/ 
https://streamsets.com/ (is not so free)
https://airflow.apache.org/

## take an existing UI from visual CI/CD systems

Another type of tool that allow visual flows/pipelines are the CI ones.
https://argoproj.github.io/


## take a system and build the UI

The last resort is to take an existing similar system and build our own UI over it.


# other examples

All the distributed systems have the same problem as DejaQ, here are a few already made frameworks that we can use or inspire from

https://github.com/kyma-incubator/octopus/blob/master/README.md (https://kyma-project.io/blog/2020/1/16/integration-testing-in-k8s)
https://coreos.com/blog/new-functional-testing-in-etcd.html
https://github.com/jepsen-io/jepsen
https://wiki.crdb.io/wiki/spaces/CRDB/pages/73138579/Testing+CockroachDB 
https://cassandra.apache.org/doc/latest/development/testing.html#dtests created dtest framework
https://www.confluent.io/blog/apache-kafka-tested/ created ducktape 
https://github.com/RedisLabsModules/RLTest
https://github.com/bloomberg/powerfulseal

