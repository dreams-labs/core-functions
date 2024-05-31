# Dreams Labs Core Python Functions

these are split into multile subfiles based on use case:
* dreams_core includes standard functions that should be useful across many use cases, such as help with formatting and importing secrets
* BigQuery is functions related to bigquery
* Dune includes functions related to dune

### Testing

After having installed the requirements, you can run the tests with the following command:

```bash
pytest
```
If you want to add test coverage, you can run the following command:

```bash
pytest --cov --cov-config=.coveragerc
```

To view the coverage report in web browser, you can run the following command:

```bash
pytest --cov --cov-report html --cov-config=.coveragerc
```

Then you can see the coverage report by opening the file `htmlcov/index.html` in your browser.