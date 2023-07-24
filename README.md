# APie Simple ETL

This repo is a compilation of Python Classes to help Extract, Load and Transform data into a DataWarehouse.
For the basic example we will be using Snowflake as DataWarehouse.

## Dev Setup

- Install ASDF to keep versions consistent
  - Install Python 3.11.4
  - Install Poetry 1.4.2

- Run
    '''poetry install'''
- Run
    '''poetry run pre-commit install'''
- copy .env.example as .env into 'tests' folder and enter the correct value

## Folders and Arch definition

All classes will be separeted in 3 folders:

- Extract
- Load
- Transform

That defines the responsibiliy of the Classes

## Inheritance

In order to create a pipeline, you should create a class that inherits both the extract class and the load class (transform is optional).
All classes are defined to provide simple methods and let the pipeline be something like:

```python
    data = obj.extract
    data = obj.transform
    obj.load(data)
```

## Changing DataWarehouse

If you want to use other DataWarehouse you just need to change the "Load" class, and provide two public Methdos:

- Load
- Get last date

Since they are the only ones that will be used on the other classes
