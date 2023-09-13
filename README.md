# APie Simple ETL

This repo is a compilation of Python Classes to help Extract data from a source (a database, a file, a data lake, or other) and make the minimal necessary transformations to load it into your Data Warehouse.

## Dev Setup

- Install [ASDF](https://asdf-vm.com/guide/getting-started.html) to keep versions consistent
  - Install Python 3.11.4
    ```bash
    asdf plugin add python
    asdf install python 3.11.4
    ```
  - Install Poetry 1.5.1
    ```bash
    asdf plugin-add poetry https://github.com/asdf-community/asdf-poetry.git
    asdf install poetry 1.5.1
    ```
- In the source repository run:
    ```bash
  poetry install
  poetry run pre-commit install
    ```
- Copy .env.example as .env into the 'tests' folder and enter the correct value for each variable

> Each class by itself does not do anything. To see the code running, try running one of the Pipelines in the folder ./src/pipelines

## Folders and Arch definition

There are two main folders at the root of the project:
- src
  - Here we have all the code
- tests
  - Here, we have all the tests, configuration files, fixtures, and anything else necessary to run tests on the code.

### src folder
Inside the src folder, we have four main folders
- extract
  - Here, we have the definition of the extract classes and the declaration of the interface
- load
- transform
- pipeline

### tests folder
To simplify the import statements on your tests, every 'test_*.py' file is placed in the tests folder root.
Any other definition necessary to run the tests will be placed into the respective folder.

## Creating a Pipeline
The main objective when defining each of the base classes is that your pipeline can be defined as:
```python
    from ../extract/choose_one_extract_class import ExtractClass
    from ../transform/choose_one_transform_class import TransformClass
    from ../load/choose_one_load_class import LoadClass

    # the **kwargs depend on the class you choose
    extract_obj = ExtractClass(**kwargs)
    transform_obj = TransformClass(**kwargs)
    load_obj = LoadClass(**kwargs)

    extracted_data = extract_obj.extract(**kwargs)
    transformed_data = transform_obj.transform(extracted_data, **kwargs)
    load_obj.load(transformed_data, **kwargs)

```
> Not all pipelines will need a transformation before the load

## How to create a new class
1. When creating a new class, you need to inherit the interface for that step, so if you want to extract data, you need to inherit the extract_interface
2. Develop all the methods defined in the interface and any other necessary methods
3. Create unit tests for your class
4. Open a PR
