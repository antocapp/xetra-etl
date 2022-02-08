## Overall

Code repository on github with python and docker -> docker image repository (dockerhub) -> image pulled from orchestration tool (apache airflow or flows for kubernetes) -> execution platform (kubernetes platform) with docker container, within we have configs and secrets -> orchestration will extract from source bucket, process and report on S3 target

## Steps

- Set up a virtual environment for the python project
- Set up an AWS account
- Jupyter Notebook to first draft of the script
- Functional vs OOP
- Importance of Testing
- Functional with first draft
- OOP and further requirements
- OOP code design
- Set up dev env (github repo, and code on VSC)
- Implement a class frame
- Implement logging functinalities according to py best practices
- Coding (Clean code, functionality, linting, unit tests, integration tests)
- Set up dependency management with pipenv
- Performance tuning with profiling and timing
- Create Dockerfile + Push on deckrhub
- Run in producton with minikube or argo workflows


## Venv

- venv
- virtualenv
- pipenv (when you develop python application, so we will use this!)

### setup

pip install pipenv
cd xetra/
pipenv shell --python python3

pipenv --venv
>> /Users/antoniocappiello/.local/share/virtualenvs/xetra-b2W5lj4s

pipenv install pandas



to activate the env, once in the folder, simply type: pipenv shell

## AWS setup

get a user with S3 permissions, save access keys csv, and set ENV variables

export AWS_ACCESS_KEY_ID="AK......."
export AWS_SECRET_ACCESS_KEY="at.........."

install cli for aws

pipenv install awscli


access to public xetra dataset:
aws s3 ls deutsche-boerse-xetra-pds/2021-04-21 --recursive --no-sign-request

outputs the file for that day:
```
2021-04-22 02:30:03        136 2021-04-21/2021-04-21_BINS_XETR00.csv
2021-04-22 02:30:04        136 2021-04-21/2021-04-21_BINS_XETR01.csv
2021-04-22 02:30:04        136 2021-04-21/2021-04-21_BINS_XETR02.csv
2021-04-22 02:30:04        136 2021-04-21/2021-04-21_BINS_XETR03.csv
2021-04-22 02:30:05        136 2021-04-21/2021-04-21_BINS_XETR04.csv
2021-04-22 02:30:05        136 2021-04-21/2021-04-21_BINS_XETR05.csv
2021-04-22 02:30:05        136 2021-04-21/2021-04-21_BINS_XETR06.csv
2021-04-22 02:30:06    1557342 2021-04-21/2021-04-21_BINS_XETR07.csv
2021-04-22 02:30:06    1190780 2021-04-21/2021-04-21_BINS_XETR08.csv
2021-04-22 02:30:06    1203011 2021-04-21/2021-04-21_BINS_XETR09.csv
2021-04-22 02:30:07    1033010 2021-04-21/2021-04-21_BINS_XETR10.csv
2021-04-22 02:30:07    1113641 2021-04-21/2021-04-21_BINS_XETR11.csv
2021-04-22 02:30:07    1064793 2021-04-21/2021-04-21_BINS_XETR12.csv
2021-04-22 02:30:07    1408780 2021-04-21/2021-04-21_BINS_XETR13.csv
2021-04-22 02:30:08    1353421 2021-04-21/2021-04-21_BINS_XETR14.csv
2021-04-22 02:30:08    1194468 2021-04-21/2021-04-21_BINS_XETR15.csv
2021-04-22 02:30:08        136 2021-04-21/2021-04-21_BINS_XETR16.csv
2021-04-22 02:30:08        136 2021-04-21/2021-04-21_BINS_XETR17.csv
2021-04-22 02:30:08        136 2021-04-21/2021-04-21_BINS_XETR18.csv
2021-04-22 02:30:09        136 2021-04-21/2021-04-21_BINS_XETR19.csv
2021-04-22 02:30:09        136 2021-04-21/2021-04-21_BINS_XETR20.csv
2021-04-22 02:30:09        136 2021-04-21/2021-04-21_BINS_XETR21.csv
2021-04-22 02:30:09        136 2021-04-21/2021-04-21_BINS_XETR22.csv
2021-04-22 02:30:10        136 2021-04-21/2021-04-21_BINS_XETR23.csv
```

## Jupyter notebook to play and analyze data

pipenv install jupyter

install also boto3 to have access to s3 buckets


pipenv install boto3
jupyter notebook


## Save report to S3 bucket

create bucket

pipenv shell
pipenv install pyarrow

## Code improvements

Parameterize

## Code design

### Functional vs OOP

Functional: used where the state is not a factor and mutable data are little involved, ETL can be done by chaining small functions

Objects: objects represent things, and have attributes containing data. Methods (functions) manipulate attributes -> easier implement real world scenario.
4 principles:

- Encapsulation
- Abstraction
- Inheritance
- Polymorphism (child class can use methods from parent class but can be modified)

What approac to choose for ETL jobs? OOP give us a strong feature set. 

## Software testing

Benefits:

- Reduces bugs
- Prevent regression (you don't have to remember all the test, the cost of manual test increase linearly)
- Writing testable code improves overall quality
- Enhances documentation
- Helps code reviewers
- Easier to add more features
- Debug edge cases

### Types of testing

Unit testing: individual tests, such as functions and classes
Integration testing: correctly connect components
System testing: conducted on the complete application, to see if the requirements are met
Acceptance testing: on the complete application, done by the customers

## Architectur design

Infrastructure
Adapter Layer
Application Layer
Domain Layer

Let's start with the adapter layer (communication via API, for example)

## OOP design principles

- Compositione over inheritance: code should be reused by compositions of classes
- Encapsulaate what varies: minimize the volume of code which may need editing
- Invesrion of control: separate the what to do from the when to do
- SOLID
- DRY (don't repeat yourself): only write once and reuse it
- YAGNI (You Aren't Gonna Needi It): implement things only when you need them

## Further requirements

- Logging (config with yaml file)
- Exception handling: the required data must meet the requirements and must be in time
- entrypoint for the orchestration tool to run the code
- configuration (use configuration file instead of hardcoding parameters, so we don't have to change code overtime)
- meta file for job control


## OOP

the cons of using functional approach is to have duplicated code

we'll rewrite functions to objects

s3.py : S3BucketConnector()
meta_process.py: MetaProcess()
xetra_transformer.py : XetraETL()
constants.py : S3FileTypes() [csv, parquet]
custom_exceptions.py

Classes have:

- instance methods
- instance attributes
- arguments 


## Create git repository



## Cloudwath alarm
Weekly trigger


## Lambda handler

def lambda_handler(event, context):
    function()

## Python Logging

DEBUG, INFO, WARNING, ERROR and CRITICAL
config file (ini, json, yaml)

`pipenv install pyyaml`

## Pythonpath

To import the package from everywhere in the system
add the project folder to pythonpath
export PYTHONPATH="... xetra"

## Clean code

Write pythonic (Dunder methods, Context managers, decorators, comprehensions)

## Unit testing

Use pytest: test each callable method
Goal: 100% test coverage

use fake or in-memory db to test database connections, or `moto` to mock S3

```bash
pipenv install coverage
coverage run -m unittest discover -v
```

```
coverage report -m

Name                                           Stmts   Miss  Cover   Missing
----------------------------------------------------------------------------
tests/__init__.py                                  0      0   100%
tests/common/__init__.py                           0      0   100%
tests/common/test_meta_process.py                117      1    99%   327
tests/common/test_s3.py                          109      1    99%   244
tests/integration_test/__init__.py                 0      0   100%
tests/transformers/__init__.py                     0      0   100%
tests/transformers/test_xetra_transformer.py     132      1    99%   297
xetra/__init__.py                                  0      0   100%
xetra/common/__init__.py                           0      0   100%
xetra/common/constants.py                         10      0   100%
xetra/common/custom_exceptions.py                  2      0   100%
xetra/common/meta_process.py                      40      0   100%
xetra/common/s3.py                                41      0   100%
xetra/transformers/__init__.py                     0      0   100%
xetra/transformers/xetra_transformer.py           75      0   100%
----------------------------------------------------------------------------
TOTAL                                            526      3    99%

```


## Integration testing 
 Test how all the components work together, using real aws instead of mock


 create 2 buckets for test (src and trg)