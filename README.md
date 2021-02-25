# petfinder-sdk

A high-performance, modern client for retrieving data from the Petfinder API.

The petfinder API provides access to detailed records for millions of pets, and anyone who requests an API key can access a minimum of 100,000 records per day. Petfinder-sdk provides a clean and efficient interface for accessing that data.

## Features

- Supports both standard and async usage. 
- It's fast. Even in "standard" mode the client will execute batches of requests concurrently, so you get the benefits of async execution without needing to write async code.
- It's efficient. To reduce duplicate requests (which count against your daily API quota), petfinder-sdk comes with a built-in requests cache.
- It's user-friendly. Results are provided as a pandas DataFrame, so you can immediately analyze them or export the data into a variety of formats.

## Getting started

```bash
$ pip install petfinder-sdk
```

Then, go here to get a petfinder API key:
https://www.petfinder.com/developers/
