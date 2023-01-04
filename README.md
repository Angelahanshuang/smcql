# SMCQL : Secure Querying for Federated Databases

--------------------------------------------------------------------------------
Author
--------------------------------------------------------------------------------

SMCQL is developed and currently maintained by [Johes Bater](mailto:johes@u.northwestern.edu), under the direction of Jennie Rogers.  It translates SQL statements into <a href="http://oblivm.com/index.html">ObliVM</a> programs for secure query evaluation.

--------------------------------------------------------------------------------
Disclaimer
--------------------------------------------------------------------------------

The code is a research-quality proof of concept, and is still under development for more features and bug-fixing.

--------------------------------------------------------------------------------
Requirements
--------------------------------------------------------------------------------
* PostgreSQL 9.5+
* Apache Calcite 1.8+
* Apache Maven 3+
* Oracle Java 8+
* JavaCC 5.0+
* Python 2.7+

--------------------------------------------------------------------------------
Setup
--------------------------------------------------------------------------------
Clone the repository:

	$ git clone https://github.com/smcql/smcql.git

Install the dependencies as needed:

* Install PostgreSQL:

	`$ sudo apt-get install postgresql postgresql-contrib`

* Create a user named smcql and switch to that user.


	`$ sudo useradd smcql`

	`$ sudo su smcql`
	
	
* Create a superuser PostgreSQL role for SMCQL: 

	`$ sudo su - postgres`  
	`$ createuser -sPE smcql`  
	`$ exit`  
	
* Install Maven: 

	`$ sudo apt-get install maven`

* Install Java: 

	`$ sudo apt-get install default-jdk`

Edit the configuration files in the repo as needed:

* conf/setup.localhost
* conf/connections/localhost

This configures your local environment. Note that you should insert your PostgreSQL password for smcql here. You also may want to add setup.localhost to your .gitignore to avoid pushing your password.

Start up PostgreSQL and run the following command in the smcql home directory:

    $ ./setup.sh

This sets up the test databases in PostgreSQL. 

--------------------------------------------------------------------------------
Running the example queries
--------------------------------------------------------------------------------
Run the following commands for the respective queries:

    Query 1: Comorbidity
    $ ./build_and_execute.sh conf/workload/sql/comorbidity.sql testDB1 testDB2 

    Query 2: Aspirin Count
    $ ./build_and_execute.sh conf/workload/sql/aspirin-count.sql testDB1 testDB2

    Query 3: Recurrent C.Diff
    $ ./build_and_execute.sh conf/workload/sql/cdiff.sql testDB1 testDB2 

Note that these queries are CPU-intensive. They may take several minutes to run, depending on hardware.

--------------------------------------------------------------------------------
Running SMCQL with your own data and schema
--------------------------------------------------------------------------------
Refer to conf/workload/testDB for the necessary files:

1. create_test_dbs.sh - This script creates and populates the PostgreSQL databases that house the data
2. test_schema.sql - This SQL query sets the schema, as well as the security level for each attribute

The above files are an automated example for setting up the test databases and annotated schema. You can choose to set these up manually, or to use existing databases. Please note that you must set the security levels for your attributes. Look at the bottom of test_schema.sql for an example. Remember that each attribute must be set as either a public, protected, or private variable.


--------------------------------------------------------------------------------
在不同的机器上运行这个示例查询
--------------------------------------------------------------------------------
1. 为远程主机创建新的配置文件：

* conf/setup.remote
* conf/connections/remote-hosts

2. 确保您有三台可直接访问彼此的机器。

3. 在两台计算机的本地 PostgreSQL 实例中填充您的数据。这些将是您的两个工作人员，因此每个人都有自己的数据。

4. 确保您的两个工作人员都正确地设置了他们的模式(如前一节所详细说明的)。请记住，架构和安全注释对于这两台机器必须是相同的。

5. 在第三台计算机（诚实的经纪人）上，确保您拥有存储库的副本，并且配置文件填充了正确的Worker配置信息。

6. 在诚实代理上从SMCQL存储库运行示例命令 Run the example command on the honest broker, from the SMCQL repository:

    `$ ./build_and_execute.sh conf/workload/sql/comorbidity.sql remoteDB1 remoteDB2`

Notes:

* Machine 1: 包含PostgreSQL数据库“remoteDB1”和正确的架构
* Machine 2: 包含PostgreSQL数据库“remoteDB2”和正确的架构
* Machine 3: 包含指定“ RemoteDB1”和“ RemoteDB2”的位置和连接信息的配置文件

--------------------------------------------------------------------------------
References
--------------------------------------------------------------------------------

*SMCQL*:

J. Bater, G. Elliott, C. Eggen, S. Goel, A. Kho, and J. Rogers, “SMCQL: Secure Querying for Federated Databases.”, VLDB, 10(6), pages 673-684, 2017.

*ObliVM*:

C. Liu, X. S. Wang, K. Nayak, Y. Huang, and E. Shi, “ObliVM : A Programming Framework for Secure Computation,” Oakland, pp. 359–376, 2015.

--------------------------------------------------------------------------------
Notes
--------------------------------------------------------------------------------

See [slides](SMCQL_Slides.pdf) presented at VLDB 2017 for more details.
