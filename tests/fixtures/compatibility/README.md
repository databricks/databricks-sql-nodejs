This directory contains fixtures for testing different data types support.

SQL files contain table structure and sample data, other files contain raw
responses for different combination of options (with or without Arrow support enabled,
with or without Arrow native types). In all cases, data should be decoded in
exactly the same way.

Known issues:

- with Arrow disabled _or_ with Arrow native types disabled:
  - date values are not properly serialized in nested structures, so complex types cannot be JSON-decoded;
    therefore this case is not represented in this test set
  - any non-string type used as map key is not properly serialized and cannot be JSON-decoded;
    therefore this case is not represented in this test set
